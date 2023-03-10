import ciso8601
import datetime as dt
import json
import logging
import oracledb
import os
import pandas as pd
import warnings
import numpy as np

from dotenv import load_dotenv
from ingestion_integration_repo.ingestion_core_repo.connectors import Connectors
from ingestion_integration_repo.ingestion_core_repo.GCPSecretManager import SecretManager
from sqlalchemy import create_engine

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")
load_dotenv()
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)



class OracleDatabaseConnection(Connectors):

    def __init__(self, **kwargs) -> None:

        logger.info("Creating connection")
        self.table_details = kwargs
        connection_details = self.get_connection_details(kwargs["connections"][0])

        self.engine = create_engine(f'''oracle+cx_oracle://
                                        {connection_details["user"]}:{connection_details["password"]}
                                        @{connection_details['host']}
                                        /{connection_details['db']}''')

        self.connection = oracledb.connect(
            user=connection_details["user"],
            password=connection_details["password"],
            dsn=f"{connection_details['host']}:{connection_details['port']}/{connection_details['db']}")

        self.cursor = self.connection.cursor()
        self.last_successful_extract = {}
        logger.info("Connection created successfully with source")

    def get_connection_details(self, secret_id):
        project_id = os.getenv("SECRET_PROJECT_ID")

        sm = SecretManager(project_id)

        connection_details = json.loads(sm.access_secret(secret_id))

        logger.info(f"Connecting to : {connection_details['host']}")
        return connection_details

    def get_schema(self, *args) -> pd.DataFrame:
        """
            Connects to the source database, and gets the source schema
            Parameters
            -----------
                *args
            Returns
            ---------
                pd.DataFrame: Schema details of source table
        """

        table_name = args[0]
        schema_details_query = f"""SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, 
        DATA_SCALE FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '{table_name.upper()}'"""

        schema_details = pd.read_sql(schema_details_query, self.connection)
        if "drop_columns" in self.table_details:
            schema_details = schema_details[~schema_details["COLUMN_NAME"].isin(self.table_details['drop_columns'])]

        schema_details.loc[schema_details['DATA_SCALE'] > 0, ['DATA_TYPE']] = 'FLOAT'
        return schema_details[["COLUMN_NAME", "DATA_TYPE"]]

    def get_incremental_clause(self, incremental_columns: dict, last_successful_extract: dict) -> str:

        """
        Creates the incremental clause to be added to where clause to fetch latest data

        Parameters
        ------------
            incremental_columns : dict
                Required Keys:
                    - column_name : [column_details]
            last_successful_extract: dict
                Example value : 
                    - {
                        last_fetch_incremental_column1: last_fetch_incremental_value1,
                        last_fetch_incremental_column2: last_fetch_incremental_value2,

                    }
        Returns
        ----------
        str : incremental_clause
        """
        incremental_clause = ""
        logger.info("Adding incremental clause to query")

        for incremental_column_name in incremental_columns:

            incremental_column_name = incremental_column_name.lower()
            incremental_column = incremental_columns[incremental_column_name]
            if incremental_column_name in last_successful_extract:

                if "column_type" in incremental_column and incremental_column["column_type"] == "timestamp":
                    incremental_clause += f"""  {incremental_column_name} >= 
                                        TO_DATE('{last_successful_extract[incremental_column_name]}', 
                                        '{incremental_column["column_format"]}') """
                elif incremental_column["column_type"] == "int":
                    incremental_clause += f""" {incremental_column_name} >= {last_successful_extract[incremental_column_name]}"""
                elif incremental_column["column_type"] == "str":
                    incremental_clause += f""" {incremental_column_name} >= '{last_successful_extract[incremental_column_name]}'"""

                incremental_clause += " and "

        if incremental_clause:
            incremental_clause = incremental_clause[:-4]
        return incremental_clause

    def execute_query(self, sql: str) -> pd.DataFrame:
        """
            Executes given query and returns the dataframe
            Parameters
            -------------
                sql: str
                    Query to be execute
            Returns
            ----------
            DataFrame: Query result
        """
        result = pd.read_sql(sql, self.connection)
        return result

    def create_dynamic_limit_query(self, incremental_clause, table_name, group_by_columns):
        select_clause = ""
        group_by_clause = ""
        order_by_clause = ""

        for column in group_by_columns:
            column_details = group_by_columns[column]

            if "column_type" in column_details and column_details["column_type"] == "timestamp":
                select_clause += f"  TRUNC( {column}, '{column_details['group_by_format']}') as {column} ,"
                group_by_clause += f"  TRUNC( {column}, '{column_details['group_by_format']}') ,"
            else:
                select_clause += f"  {column} ,"
                group_by_clause += f" {column} ,"
            order_by_clause += f" {column} ,"

        select_clause = select_clause[:-1]
        group_by_clause = group_by_clause[:-1]
        order_by_clause = order_by_clause[:-1]

        if incremental_clause:
            dynamic_limit_query = """select count(*) as ROW_COUNT, {} from {} where {} group by {} """
            dynamic_limit_query = dynamic_limit_query.format(select_clause, table_name,
                                                             incremental_clause, group_by_clause)

        else:
            dynamic_limit_query = """select count(*) as ROW_COUNT, {} from {} group by {} """

            dynamic_limit_query = dynamic_limit_query.format(select_clause, table_name,
                                                             group_by_clause)

        if order_by_clause:
            order_by_clause += " asc"

            dynamic_limit_query += f" order by {order_by_clause}"

        return dynamic_limit_query

    def get_dynamic_limit(self, incremental_clause: dict,
                          table_name: str,
                          batch_size: int,
                          group_by_columns: dict, ):
        dynamic_limit_query = self.create_dynamic_limit_query(incremental_clause, table_name, group_by_columns)
        logger.info(f"Dynamic Limit query : {dynamic_limit_query}")
        result = self.execute_query(dynamic_limit_query)

        result.columns = [column.lower() for column in result.columns]
        logger.info(f"Created {len(result)} batches")

        return result

    def execute_batches(self,
                        query: str,
                        incremental_clause: dict,
                        table_name: str,
                        batch_size: int,
                        group_by_columns: dict
                        ) -> pd.DataFrame:

        """

        :param group_by_columns:
        :param query:
        :param incremental_clause:
        :param table_name:
        :param batch_size:
        :param group_by_column:
        :param group_by_format:
        :return:
        """
        if group_by_columns:
            dynamic_limit = self.get_dynamic_limit(incremental_clause, table_name, batch_size, group_by_columns)

            for index, row in dynamic_limit.iterrows():
                row = row.to_dict()
                updated_query = query

                for column in group_by_columns:
                    print(row[column], type(row[column]))

                    if "column_type" in group_by_columns[column] and group_by_columns[column]["column_type"] \
                            == "timestamp":

                        if row[column] is not None:
                            updated_query = query + f" and TRUNC({column}, '{group_by_columns[column]['group_by_format']}') = " \
                                                    f"TO_DATE('{row[column]}', '{group_by_columns[column]['column_format']}')"

                        else:
                            updated_query = query + f" and TRUNC({column}, '{group_by_columns[column]['group_by_format']}') is Null "

                    else:
                        if type(row[column]) is str:
                            updated_query += f"  and {column} = '{row[column]}'  "
                        elif type(row[column]) is None or row[column] is None or row[column] is np.nan:
                            updated_query += f"  and {column} is null  "

                        else:
                            updated_query += f"  and {column} = {row[column]}  "

                            print(type(row[column]), row[column], "Inside else")

                logger.info(f"Running query : {updated_query}")
                result = self.execute_query(updated_query)
                logger.info(f"Fetched {len(result)} records ")
                yield result

        else:
            logger.info(f"Running query : {query}")
            result = self.execute_query(query)
            logger.info(f"Fetched {len(result)} records ")
            yield result

    def handle_extract_error(self, args):
        pass

    def update_last_successful_extract(self, incremental_columns: list, result_df: pd.DataFrame) -> None:

        list_incremental_columns = list(incremental_columns.keys())
        df_columns = list(map(str.lower, list(result_df.columns)))
        for incremental_column in list_incremental_columns:
            if incremental_column in df_columns:
                incremental_column_last_batch_fetched_value = result_df[incremental_column.upper()].max()
                if incremental_column in self.last_successful_extract:
                    if "column_type" in incremental_columns[incremental_column] and \
                            incremental_columns[incremental_column]["column_type"] == "timestamp" \
                            and type(self.last_successful_extract[incremental_column]) is str:
                        self.last_successful_extract[incremental_column] = ciso8601.parse_datetime(
                            self.last_successful_extract[incremental_column])

                    self.last_successful_extract[incremental_column] = max(
                        self.last_successful_extract[incremental_column],
                        incremental_column_last_batch_fetched_value)
                else:
                    self.last_successful_extract[incremental_column] = incremental_column_last_batch_fetched_value
        logger.info(f"Updated last successful extract : {self.last_successful_extract}")

    def extract(self, last_successful_extract: dict, **table: dict):
        """
            Main Oracle extraction function
            Parameters
            ------------
                last_successful_extract: dict
                    - Required Keys:
                        . last_fetched_value
                table: dict
                    - Required Keys:
                        . batch_size
                        . query
                        . incremental_type
                        . incremental_column
                        . incremental_column_format
            Return
            --------
                pd.DataFrame : Extracted dataframe from source table
        """

        table = self.table_details
        if last_successful_extract:
            self.last_successful_extract = last_successful_extract

        batch_size = table["batch_size"]
        query = table["query"] + "where 1 = 1 "

        if "where_clause" in table and table["where_clause"]:
            query += f" and  {table['where_clause']}"

        if "incremental_column" in table:
            incremental_columns = table["incremental_column"]
        else:
            incremental_columns = {}

        if "group_by_columns" in table:
            group_by_columns = table["group_by_columns"]
        else:
            group_by_columns = {}

        # If there is a last successful extract then add incremental clause
        incremental_clause = ""
        if last_successful_extract:
            incremental_clause = self.get_incremental_clause(incremental_columns, last_successful_extract)
            if incremental_clause:
                query += f"and   {incremental_clause}"

        return_args = {"extraction_status": False}

        func = self.execute_batches(query, incremental_clause, table["name"], batch_size, group_by_columns)
        while True:
            try:
                result_df = next(func)
                return_args["extraction_status"] = True
                self.update_last_successful_extract(incremental_columns, result_df)

                yield result_df, return_args
            except StopIteration:
                break
            # except Exception as e:
            #     yield pd.DataFrame(), return_args
