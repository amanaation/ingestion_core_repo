import logging
import pandas as pd
import sys

sys.path.append('../')

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

from datatypes import SourceDestinationTypeMapping
from ingestion_core_repo.connectors import Connectors
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

    def __init__(self, **kwargs):
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
        self.project_id = kwargs['target_project_id']
        self.dataset_name = kwargs['target_bq_dataset_name']
        self.table_name = kwargs['target_table_name']

        self.table_id = f"{self.project_id}.{self.dataset_name}.{self.table_name}"

        # Creating BigQuery client
        self.client = bq.Client()
        self.job_config = bq.LoadJobConfig()
        self.last_successful_values = {}

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
        logger.info(f"Creating Schema : {self.table_name}")

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

    def save(self, df: pd.DataFrame) -> None:
        """
            This function writes the dataframe to bigquery

            Parameters
            ----------
                df: dataframe to write to bigquery

            Returns
            --------
            None
        """
        job = self.client.load_table_from_dataframe(
            df, self.table_id, job_config=self.job_config
        )
        job.result()
