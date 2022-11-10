from google.cloud import bigquery, storage
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
import pandas as pd
import pygsheets

class GoogleBase:
    def __init__(self, project_id='css-operations', oauth_file=None, svc_account_file=None, svc_account_info=None, ds_secret=None):
        self.project_id = project_id

        if oauth_file:
            self.creds = Credentials.from_authorized_user_file(oauth_file)
        elif svc_account_file:
            self.creds = service_account.Credentials.from_service_account_file(svc_account_file)
        elif svc_account_info:
            self.creds = service_account.Credentials.from_service_account_info(svc_account_info)
        elif ds_secret:
            from ds.utilities.secrets import secrets
            s = secrets.Secrets()
            self.creds = service_account.Credentials.from_service_account_info(s.get_secret(ds_secret))

class BigQuery(GoogleBase):
    def __init__(self, project_id='css-operations', oauth_file=None, svc_account_file=None, svc_account_info=None, ds_secret=None):
        super().__init__(project_id, oauth_file, svc_account_file, svc_account_info, ds_secret)
        self.client = bigquery.Client(credentials=self.creds, project=self.project_id)

    def run_query(self, query: str) -> pd.DataFrame:
        job = self.client.query(query)
        return job.to_dataframe()

    @staticmethod
    def __get_write_disposition(how):
        if how == 'WRITE_APPEND':
            return bigquery.WriteDisposition.WRITE_APPEND
        elif how == 'WRITE_TRUNCATE':
            return bigquery.WriteDisposition.WRITE_TRUNCATE
        elif how == 'WRITE_EMPTY':
            return bigquery.WriteDisposition.WRITE_EMPTY
        else:
            return None

    def upload_to_table_from_df(
        self, 
        df, 
        tbl_id, 
        how='WRITE_APPEND', 
        clustering_fields = None, 
        schema=None, 
        autodetect=True, 
        max_bad_records=0,
        ):

        job_config = bigquery.LoadJobConfig(
            write_disposition=self.__get_write_disposition(how),
            clustering_fields=clustering_fields,
            autodetect=autodetect,
            schema=schema,
            allow_quoted_newlines=True,
            max_bad_records = max_bad_records,
        )

        load_job = self.client.load_table_from_dataframe(df, tbl_id, job_config=job_config)

        return load_job

class GSheets(GoogleBase):
    def __init__(self, project_id='css-operations', oauth_file=None, svc_account_file=None, svc_account_info=None, ds_secret=None):
        super().__init__(project_id, oauth_file, svc_account_file, svc_account_info, ds_secret)
        self.client = pygsheets.authorize(custom_credentials=self.creds)

class CloudStorage(GoogleBase):
    def __init__(self, project_id='css-operations', oauth_file=None, svc_account_file=None, svc_account_info=None, ds_secret=None):
        super().__init__(project_id, oauth_file, svc_account_file, svc_account_info, ds_secret)
        self.client = storage.Client(credentials=self.creds, project=self.project_id)

    def upload(self, file_name: str, file_to_upload: str, bucket_name: str = "otter-sp"):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_to_upload)
    
    def upload_df(self, file_name: str, df: pd.DataFrame, bucket_name: str = "brand_science", df_format: str = 'text/csv', field_delimiter: str = '%'):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(df.to_csv(index=False, encoding='utf-8', quoting=csv.QUOTE_NONE, quotechar='',escapechar='\\', sep=field_delimiter), df_format)

    def download(self, file_name: str, download_to_path: str, bucket_name: str = "otter-sp"):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.download_to_filename(download_to_path)

    def list_files(self, bucket_name: str = "otter-sp"):
        return [b for b in self.client.list_blobs(bucket_name)]