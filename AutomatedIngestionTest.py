'''
Requirements:
pandas
sklearn
urllib3
google
'''
# Imports
import os
import pandas as pd
import urllib3
import certifi
import json
from google.cloud import bigquery

# Function to store data into Big Query
# store_to_bq(pandas, projectId.bqdataset.datasetTable)
def store_to_bq(data_to_store, bq_table_name):
    # Setup credentials and Client
    project_root = os.path.dirname(os.path.abspath(__file__))
    gcp_credentials = project_root + "/auth/gcp-data-warehouse-7e433748bff6.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_credentials
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    bq_table_id = 'gcp-data-warehouse.staging.' + bq_table_name
    job = client.load_table_from_dataframe(
        data_to_store, bq_table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

# PAYMENTS
# Get Payments data from shared csv file in drive
file_id = '1GlYrv7ex0ClxQwQ0NvJ4GTUGre7s8vtw'
file_path = 'https://drive.google.com/uc?export=download&id=' + file_id
pagamentos = pd.read_csv(file_path, names=["ClienteID", "DataDoPagamento", "Valor", "Plano"])

store_to_bq(pagamentos, 'pagamentos')  # Store into BQ

# CLIENTS
# Get Client Data from rest API
http = urllib3.PoolManager(
       cert_reqs='CERT_REQUIRED',
       ca_certs=certifi.where())  # handle certificate verification and SSL warnings
api_endpoint = 'https://demo0192734.mockable.io/clientes/'
rest_output = http.request('GET', api_endpoint)
clientes = json.loads(rest_output.data.decode('utf-8'))
clientes_df = pd.json_normalize(clientes)

store_to_bq(clientes_df, 'clientes')  # Store into BQ
