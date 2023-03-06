import logging
import pandas as pd
import sys

sys.path.append('../')

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

from ingestion_integration_repo.main.datatypes import SourceDestinationTypeMapping
from ingestion_integration_repo.ingestion_core_repo.connectors import Connectors
from dotenv import load_dotenv
from google.cloud import bigquery as bq
from google.api_core.exceptions import Conflict

load_dotenv()


class BigQuery(Connectors):
    """
    BigQuery connection class to connect, read, get schema details and write to Bigquery

    Parameters
    ----------
        kwargs: dict
            Required keys:
                gcp_project_id : GCP project ID to use
                gcp_bq_dataset_name : GCP dataset name to connect to
                target_table_name : GCP table name to write/read data from and to        

    """

    def __init__(self, dataset_name, destination_table_name, **kwargs):
        """
        Constructs all the necessary attributes for the Bigquery class

        Parameters
        ----------
            kwargs: dict
                Required keys:
                    gcp_project_id : GCP project ID to use
                    gcp_bq_dataset_name : GCP dataset name to connect to
                    target_table_name : GCP table name to write/read data from and to        
        """
        self.table_config_details = kwargs
        self.project_id = kwargs['target_project_id']
        self.dataset_name = dataset_name
        self.destination_table_name = destination_table_name

        self.table_id = f"{self.project_id}.{self.dataset_name}.{self.destination_table_name}"

        # Creating BigQuery client
        self.client = bq.Client()
        self.last_successful_values = {}

    def get_table_id(self, dataset_name, destination_table_name):
        self.table_id = f"{self.project_id}.{dataset_name}.{destination_table_name}"
        return self.table_id

    def create_dataset(self) -> None:
        """
            Create dataset in bigquery if not exists

            Returns
            ----------
            None
        """
        dataset_id = f"{self.client.project}.{self.dataset_name}"
        dataset = bq.Dataset(dataset_id)
        dataset.location = "US"
        try:
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Successfully created dataset : {self.dataset_name}")
        except Conflict:
            logger.info("Dataset already exists")

    def create_schema(self, schema_df: pd.DataFrame, source: str) -> None:
        """
            Create schema in bigquery if not exists
            Parameters
            ----------
                schema_df : Source schema details in a dataframe
                source: Name of the source e.g. oracle/bq

            Returns
            ----------
            None
        """
        logger.info(f"Creating DataSet : {self.dataset_name}")
        self.create_dataset()
        logger.info(f"Creating Schema : {self.destination_table_name}")

        schema = []
        target_types = []

        try:
            for index, row in schema_df.iterrows():
                column_name = row['COLUMN_NAME']
                column_name = column_name.strip()
                source_data_type = row['DATA_TYPE']

                target_type_mapping = SourceDestinationTypeMapping[source.lower()].value

                try:
                    target_data_type = target_type_mapping[source_data_type].value
                except:
                    target_data_type = "STRING"

                target_types.append(target_data_type)
                field = bq.SchemaField(column_name, target_data_type)
                schema.append(field)

            table = bq.Table(self.table_id, schema=schema)
            self.client.create_table(table)
            logger.info(f"Successfully created schema : {self.table_id}")
        except Conflict:
            logger.info("Schema already exists")

        return target_types

    def get_schema(self, **kwargs) -> None:
        pass

    def execute(self, sql: str, project_id: str) -> pd.DataFrame:
        """
        This function is to return dataframe out of query result
        Parameters
        ----------
            sql: str
                 query string to return result
            project_id: str
                GCP project ID
        returns:
            df: pd.DataFrame
                 dataframe with source data
        """
        return pd.read_gbq(sql, project_id=project_id)

    def extract(self, sql, project_id) -> None:
        return self.execute(sql, project_id)

    def get_on_clause(self, merge_columns):

        _on_clause = ""
        for column in merge_columns:
            _on_clause += f" _target.{column} = _source.{column} and"

        _on_clause = _on_clause [:-3]
        return _on_clause

    def get_update_upsert_clause(self, columns):
        _update_clause = "update set "
        for column in columns:
            _update_clause += f"  {column} = _source.{column}  ,"

        _update_clause = _update_clause[:-1]
        return _update_clause

    def get_upsert_insert_clause(self, columns):
        insert_columns = ', '.join(columns)
        insert_values = "_source." + ', _source.'.join(columns)

        _insert_clause = f" insert  ( {insert_columns} )  values ( {insert_values} ) "

        return _insert_clause

    def delete_temp_table(self, table_name):
        logger.info(f"Deleting temp table : {table_name}")
        _delete_query = f"drop table  {table_name}"
        self.execute(_delete_query, self.project_id)
        logger.info(f"Successfully deleted temp table : {table_name}")

    def upsert_data(self, source_table_id, target_table_id, source_schema_df):
        logger.info("Merging temp table into destination table")
        merge_columns = self.table_config_details['primary_columns']
        schema_columns = source_schema_df['COLUMN_NAME']

        if 'incremental_columns' in self.table_config_details:
            merge_columns += list(self.table_config_details['incremental_columns'].keys())

        _on_clause = self.get_on_clause(merge_columns)
        _update_clause = self.get_update_upsert_clause(schema_columns)
        _insert_clause = self.get_upsert_insert_clause(schema_columns)

        _merge_query = f"""                            
                    MERGE {target_table_id} _target
                    
                    USING {source_table_id} _source
                    ON 
                        {_on_clause}
                    WHEN MATCHED THEN
                        {_update_clause}
                    WHEN NOT MATCHED THEN
                        {_insert_clause} 
        
        """
        print(_merge_query)

        self.execute(_merge_query, self.project_id)
        logger.info(f"Successfully merged source and destination table")
        # self.delete_temp_table(source_table_id)

    def save(self, df: pd.DataFrame, write_mode='a') -> None:
        """
            This function writes the dataframe to bigquery

            Parameters
            ----------
                df: dataframe to write to bigquery

            Returns
            --------
            None
            :param write_mode:
        """

        job_config = bq.LoadJobConfig()
        job = self.client.load_table_from_dataframe(
            df, self.table_id, job_config=job_config
        )
        job.result()
