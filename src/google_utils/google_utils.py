from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
import pandas as pd
import pygsheets

class GoogleBase:
    def __init__(self, project_id, oauth_file=None, svc_account_info=None):
        self.project_id = project_id

        if oauth_file:
            self.creds = Credentials.from_authorized_user_file(oauth_file)
        elif svc_account_info:
            self.creds = service_account.Credentials.from_service_account_info(svc_account_info)

class BigQuery(GoogleBase):
    def __init__(self, project_id='css-operations', oauth_file=None, svc_account_info=None):
        super().__init__(project_id, oauth_file, svc_account_info)
        self.client = bigquery.Client(credentials=self.creds, project=self.project_id)

    def run_query(self, query: str) -> pd.DataFrame:
        job = self.client.query(query)
        return job.to_dataframe()

class GSheets(GoogleBase):
    def __init__(self, project_id='css-operations', oauth_file=None, svc_account_info=None):
        super().__init__(project_id, oauth_file, svc_account_info)
        self.client = pygsheets.authorize(custom_credentials=self.creds)