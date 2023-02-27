import logging
import oracledb
import pandas as pd
import os
import warnings
import logging

from dotenv import load_dotenv
from sqlalchemy import create_engine
from ingestion_core_repo.connectors import Connectors

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")
load_dotenv()

class OracleDatabaseConnection(Connectors):

    def __init__(self, **kwargs) -> None:
        
        logger.info("Creating connection")
        connection_details = self.get_connection_details()

        self.engine = create_engine(f'''oracle+cx_oracle://
                                        {connection_details["user"]}:{connection_details["password"]}
                                        @{connection_details['host']}
                                        /{connection_details['DB']}''')
        
        self.connection = oracledb.connect(
            user=connection_details["user"],
            password=connection_details["password"],
            dsn=f"{connection_details['host']}:{connection_details['port']}/{connection_details['DB']}")

        self.cursor = self.connection.cursor()
        self.last_successful_extract = {}
        logger.info("Connection created successfully with source")

    def get_connection_details(self):
        conn_details = {"user": os.getenv("DBUSER"),
                "password": os.getenv("PASSWORD"),
                "host": os.getenv("HOST"),
                "port": os.getenv("PORT"),
                "DB": os.getenv("DB")
                }

        return conn_details

    def get_schema(self, *args) -> pd.DataFrame:
        """
            Connects to the source database, and gets the source schema
            Parameters
            -----------
                table_name: Name of the source table
            Returns
            ---------
                pd.DataFrame: Schema details of source table
        """

        table_name = args[0]
        schema_details_query = f"""SELECT column_name, data_type, nullable
                                  FROM USER_TAB_COLUMNS
                                  WHERE table_name = '{table_name.upper()}' """

        schema_details = pd.read_sql(schema_details_query, self.connection)
        return schema_details

    def get_incremental_clause(self, incremental_columns: dict, last_successful_extract: dict) -> str:

        """
        Creates the inmcremental clause to be added to where clause to fetch latest data

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
        for incremental_column_name in incremental_columns:

            incremental_column_name = incremental_column_name.lower()
            incremental_column = incremental_columns[incremental_column_name]
            if incremental_column_name in last_successful_extract:
                logger.info("Adding incremental clause to query")

                if incremental_column["column_type"] == "timestamp":
                    incremental_clause += f"""  {incremental_column_name} > 
                                        TO_DATE('{last_successful_extract[incremental_column_name]}', 
                                        '{incremental_column["column_format"]}') """

                elif incremental_column["column_type"] == "id":
                    incremental_clause += f""" {incremental_column_name} > {last_successful_extract[incremental_column_name]}"""

                incremental_clause += " and"

        if incremental_clause:
            incremental_clause = incremental_clause[:-4]
        return incremental_clause

    def execute_query(self, sql: str) -> pd.DataFrame:
        """
            Executes given query and returns the dataframe
            Parameters
            -------------
                query: str
                    Query to be execute
            Returns
            ----------
            DataFrame: Query result
        """
        result = pd.read_sql(sql, self.connection)
        return result

    def add_dynamic_limit(self, 
                          incremental_clause, 
                          table_name, 
                          batch_size, 
                          group_by_column, 
                          group_by_format):

        dynamic_limit = self.get_dynamic_limit(incremental_clause, table_name, batch_size, group_by_column, group_by_format)
        print(dynamic_limit)

    def create_dynamic_limit_query(self, incremental_clause, table_name, batch_size, group_by_column, group_by_format):
        if incremental_clause:
            dynamic_limit_query = """select TRUNC({}, '{}') as group_by_timestamp, 
                                    count(*) as row_count from {} where {} group by TRUNC({}, '{}')  
                                    order by group_by_timestamp desc"""
            dynamic_limit_query = dynamic_limit_query.format(group_by_column, group_by_format, table_name, incremental_clause, group_by_column,  group_by_format)

        else:
            dynamic_limit_query = """select TRUNC({}, '{}') as group_by_timestamp, 
                                    count(*) as row_count from {} group by TRUNC({}, '{}')  
                                    order by group_by_timestamp desc"""

            dynamic_limit_query = dynamic_limit_query.format(group_by_column, group_by_format, table_name, group_by_column,  group_by_format)

        return dynamic_limit_query

    def get_dynamic_limit(self, incremental_clause, table_name, batch_size, group_by_column, group_by_format):
        group_by_formats = ["hh", "dd", "iw", "mm", "yy"]
        group_by_formats_description = {"hh": "Hourly", "dd": "Daily", "iw": "Weekly","mm": "Monthly", "yy": "Yearly"}

        for i in range(len(group_by_formats)):
            group_by_format = group_by_formats[i]
            dynamic_limit_query = self.create_dynamic_limit_query( incremental_clause, table_name, batch_size, group_by_column, group_by_format)

            result = self.execute_query(dynamic_limit_query)
            if result["ROW_COUNT"].max() >= batch_size:
                if i:
                    i = i-1
                break
        group_by_format = group_by_formats[i]
        logger.info(f"Extracting Data by grouping it in {group_by_formats_description[group_by_format]} batches")

        final_dynamic_limit_query = self.create_dynamic_limit_query( incremental_clause, table_name, batch_size, group_by_column, group_by_format)
        result = self.execute_query(final_dynamic_limit_query)
        logger.info(f"Created {len(result)} batches")

        return result

    def execute_batches(self, 
                        query: str, 
                        incremental_clause, 
                        table_name,
                        batch_size: int, 
                        incremental_columns: dict, 
                        group_by_column, 
                        group_by_format) -> pd.DataFrame :

        incremental_columns = list(incremental_columns.keys())
        incremental_columns = ', '.join(incremental_columns)

        dynamic_limit = self.get_dynamic_limit(incremental_clause, table_name, batch_size, group_by_column, group_by_format)
        if incremental_clause:
            query += f" and {incremental_clause} "
        
        for index, row in dynamic_limit.iterrows():
            group_by_timestamp = row[0]
            next_group_by_timestamp = ""

            if index:
                next_group_by_timestamp = dynamic_limit.iloc[[index-1]]["GROUP_BY_TIMESTAMP"].to_list()[0]

            updated_query += f" and {group_by_column} >= TO_DATE('{group_by_timestamp}', '{group_by_format}') "

            if next_group_by_timestamp:
                updated_query += f" and {group_by_column} < TO_DATE('{next_group_by_timestamp}', '{group_by_format}') "

            logger.info(f"Running query : {updated_query}")
            logger.info(f"Fetched data for {group_by_timestamp}")

            yield self.execute_query(updated_query)

    def handle_extract_error(self, args):
        pass

    def update_last_successful_extract(self, incremental_columns, result_df) -> None:
        print("Updating values")
        for incremental_column in incremental_columns:
            incremental_column_last_batch_fetched_value = result_df[incremental_column].max()
            if incremental_column in self.last_successful_extract:
                self.last_successful_extract[incremental_column] = max(self.last_successful_extract[incremental_column], incremental_column_last_batch_fetched_value)
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

        if last_successful_extract:
            self.last_successful_extract = last_successful_extract

        batch_size = table["batch_size"]
        query = table["query"]
        incremental_columns = table["incremental_column"]

        # If there is a last successfull extract then add incremental clause
        incremental_clause = ""
        if last_successful_extract:
            incremental_clause = self.get_incremental_clause(incremental_columns, last_successful_extract)

            if "where" in query.lower():
                query += f"and   {incremental_clause}"
            else:
                query += f" where {incremental_clause}"

        return_args = {"extraction_status": False}

        func = self.execute_batches(query, incremental_clause, table["name"], batch_size, incremental_columns, table["grouby_column"], table["grouby_format"])
        while True:
            try:
                result_df = next(func)
                return_args["extraction_status"] = True
                self.update_last_successful_extract(incremental_columns, result_df)

                yield result_df, return_args
            except StopIteration:
                break
            except Exception as e:
                yield pd.DataFrame(), return_args 


