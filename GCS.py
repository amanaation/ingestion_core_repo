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

from connectors.connectors import Connectors
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from io import StringIO
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()


class GCS(Connectors):
    def __init__(self, **kwargs) -> None:

        self.client = storage.Client()
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

        content = StringIO(content)  # tranform bytes to string here

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
        destination_generation_match_precondition = 0
        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name,
            if_generation_match=destination_generation_match_precondition,
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

    def handle_extract_error(self, args):
        self.move_blob(self.bucket_name, args["file_name"], os.getenv("GCS_ERROR_BUCKET_NAME"), args["file_name"])

    def update_last_successful_extract(self, max_timestamp):
        new_timestamp = ciso8601.parse_datetime(max_timestamp)

        if "max_timestamp" not in self.last_successful_extract:
            self.last_successful_extract["max_timestamp"] = str(max_timestamp)

        else:
            last_max_timestamp = ciso8601.parse_datetime(self.last_successful_extract["max_timestamp"])
            self.last_successful_extract["max_timestamp"] = str(max(new_timestamp, last_max_timestamp))

    def extract(self, last_successful_extract, **kwargs):

        return_args = {"extraction_status": False, "file_name": ""}
        last_max_timestamp = ""

        if last_successful_extract:
            self.last_successful_extract = last_successful_extract

        blobs = self.bucket.list_blobs(prefix=self.file_path)
        for blob in blobs:

            # try:
            if not blob.name.endswith("/"):
                print("file : ", blob.name)

                return_args["file_name"] = blob.name
                file_path = f"gs://{self.bucket_name}/{blob.name}"
                file_data = self.read_file(file_path)
                file_data = self.rectify_column_names(file_data)

                return_args["extraction_status"] = True
                self.move_blob()
                # self.update_last_successful_extract(str(blob.updated))
                yield file_data.head(), return_args

            # except Exception as e:
            #     logger.info(f"Error occurred while reading file {blob.name}")
            #     logger.info(f"Moving file to error bucket ")
            #     yield pd.DataFrame({"": [], "": []}), return_args

    def save(self, df: pd.DataFrame) -> None:
        pass
