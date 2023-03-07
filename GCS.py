import ciso8601
import csv
import logging
import pandas as pd
import re
import sys
import os
import google.api_core

sys.path.append('../')

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

from ingestion_integration_repo.ingestion_core_repo.connectors import Connectors
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from io import StringIO
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()


class GCS(Connectors):
    def __init__(self, **kwargs) -> None:

        self.client = storage.Client()
        self.config_details = kwargs
        self.bucket_name = kwargs['source_gcs_bucket_name']
        self.bucket_path = f"gs://{self.bucket_name}"
        self.bucket = Bucket.from_string(f"gs://{kwargs['source_gcs_bucket_name']}", client=self.client)
        if "source_gcs_file_path" in kwargs:
            self.file_path = kwargs['source_gcs_file_path']
        else:
            self.file_path = None

        self.last_successful_extract = {}

    def get_schema(self, *args):
        table_sample_data = args[1]

        schema_details = {"COLUMN_NAME": [], "DATA_TYPE": []}
        for column in table_sample_data.columns:
            schema_details["COLUMN_NAME"].append(column)
            column_type = re.findall("\'(.*?)\'", str(type(table_sample_data[column].to_list()[0])))[0]

            schema_details["DATA_TYPE"].append(column_type)

        print("schema details : ", schema_details)
        if "source_path" not in schema_details["COLUMN_NAME"]:
            schema_details["COLUMN_NAME"].append("source_path")
            schema_details["DATA_TYPE"].append("str")

        schema_details = pd.DataFrame(schema_details)
        schema_details["NULLABLE"] = ['Y'] * len(schema_details)
        return schema_details

    def create_schema(self, *args):
        pass

    def get_files(self, path=None):
        pass

    def rectify_column_names(self, df):
        columns = list(df.columns)

        for i in range(len(columns)):
            column = columns[i]
            column = column.strip()
            if type(column[0]) is not str:
                column = "" + column[0]

            columns[i] = column
        df.columns = columns
        return df

    def read_csv(self, file_path):
        return pd.read_csv(file_path)

    def read_xml(self, file_path):
        return pd.read_xml(file_path)

    def read_json(self, file_path):
        return pd.read_json(file_path, lines=True)

    def read_file(self, file_path):
        logger.info(f"Reading file {file_path} from bucket {self.bucket_name}")

        if file_path.endswith(".csv"):
            df = self.read_csv(file_path)
        elif file_path.endswith(".xml"):
            df = self.read_xml(file_path)
        elif file_path.endswith(".json"):
            df = self.read_json(file_path)
        return df

    def read_file_as_blob(self, blob: Bucket.blob) -> pd.DataFrame:
        content = blob.download_as_string()
        content = content.decode('utf-8')

        content = StringIO(content)  # trasnform bytes to string here

        datas = csv.reader(content)
        df = pd.DataFrame(datas)
        df.columns = df.iloc[0]
        df = df[1:]

        return df

    def create_bucket(self, storage_client, bucket_name):
        try:
            storage_client.create_bucket(bucket_name)
            logger.info("Created target bucket")
        except google.api_core.exceptions.Conflict:
            logger.info("Target bucket exists")

    def move_blob(self, bucket_name, blob_name, destination_bucket_name, destination_blob_name):
        """
        Moves a blob from one bucket to another with a new name.
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The ID of your GCS object
        # blob_name = "your-object-name"
        # The ID of the bucket to move the object to
        # destination_bucket_name = "destination-bucket-name"
        # The ID of your new GCS object (optional)
        # destination_blob_name = "destination-object-name"
        """

        storage_client = storage.Client()

        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        logger.info(f"Creating target bucket {destination_bucket_name}")
        self.create_bucket(storage_client, destination_bucket_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        """
        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request is aborted if the object's
        # generation number does not match your precondition. For a destination
        # object that does not yet exist, set the if_generation_match precondition to 0.
        # If the destination object already exists in your bucket, set instead a
        # generation-match precondition using its generation number.
        # There is also an `if_source_generation_match` parameter, which is not used in this example.
        """
        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name,
        )
        source_bucket.delete_blob(blob_name)

        print(
            "File {} in bucket {} moved to following path {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )

    def get_error_folder_path(self, old_path):
        # dev/sellsthru/queue/koch/inventory/14-Feb-2023/inventory_apac.csv
        new_path = old_path.replace("queue", "error")
        return new_path

    def handle_extract_error(self, args):
        error_folder_path = self.get_error_folder_path(args["file_name"])
        self.move_blob(self.bucket_name, args["file_name"], self.bucket_name, error_folder_path)

    def update_last_successful_extract(self, max_timestamp):
        new_timestamp = ciso8601.parse_datetime(max_timestamp)

        if "max_timestamp" not in self.last_successful_extract:
            self.last_successful_extract["max_timestamp"] = str(max_timestamp)
        else:
            last_max_timestamp = ciso8601.parse_datetime(self.last_successful_extract["max_timestamp"])
            self.last_successful_extract["max_timestamp"] = str(max(new_timestamp, last_max_timestamp))

    def get_processed_folder_path(self, old_path):
        # dev/sellsthru/queue/koch/inventory/14-Feb-2023/inventory_apac.csv
        new_path = old_path.replace("queue", "processed")
        return new_path

    def get_batch_size(self):
        if 'batch_size' in self.config_details['batch_size']:
            return self.config_details['batch_size']

    def extract(self, last_successful_extract):
        print("File path : ", self.file_path)
        return_args = {"extraction_status": False, "file_name": ""}

        if last_successful_extract:
            self.last_successful_extract = last_successful_extract

        blobs = self.bucket.list_blobs(prefix=self.file_path)
        for blob in blobs:

            try:
                if not blob.name.endswith("/"):
                    print("file : ", blob.name)

                    return_args["file_name"] = blob.name
                    file_path = f"gs://{self.bucket_name}/{blob.name}"
                    file_data = self.read_file(file_path)
                    file_data = self.rectify_column_names(file_data)
                    file_data["source_path"] = [f"{self.bucket_name}/{blob.name}"] * len(file_data)

                    return_args["extraction_status"] = True
                    processed_folder_path = self.get_processed_folder_path(blob.name)
                    logger.info(f"Moving file {blob.name.split('/')[-1]} to processed folder")
                    # self.move_blob(self.bucket_name, blob.name, self.bucket_name, processed_folder_path)
                    yield file_data, return_args

            except Exception as e:
                logger.info(f"Error occurred while reading file {blob.name}")
                logger.info(f"Moving file to error bucket ")
                yield pd.DataFrame({"": [], "": []}), return_args

    def save(self, df: pd.DataFrame) -> None:
        pass
