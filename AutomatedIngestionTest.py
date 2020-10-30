'''
Requirements:
pandas
sklearn
urllib3
google
'''

import os
import pandas as pd
import urllib3
import certifi
import json
from google.cloud import bigquery

project_root = os.path.dirname(os.path.abspath(__file__))
gcp_credentials = project_root + "/auth/data-lake-294001-13757a51dc09.json"

print(gcp_credentials)

# handle certificate verification and SSL warnings
# https://urllib3.readthedocs.io/en/latest/user-guide.html#ssl
http = urllib3.PoolManager(
       cert_reqs='CERT_REQUIRED',
       ca_certs=certifi.where())

api_endpoint = 'https://demo0192734.mockable.io/clientes/'
rest_output = http.request('GET', api_endpoint)
clientes = json.loads(rest_output.data.decode('utf-8'))
clientes_df = pd.json_normalize(clientes)

# Big query ingestion
bq_table_id = 'data-lake-294001.Landing.clientes'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_credentials
client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

job = client.load_table_from_dataframe(
    clientes_df, bq_table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(bq_table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), bq_table_id
    )
)
