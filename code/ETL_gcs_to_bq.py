from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_query
from prefect_gcp.cloud_storage import GcsBucket

# CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{bq_dataset_name}.{bq_table_name}`

@flow
def populate_bq(gcp_credentials, bucket_name, bq_dataset_name, bq_table_name, year):
    
    query = f'''
        CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-375312.ds_zoomcamp.test`
        OPTIONS (
        format = 'PARQUET',
        uris = ['gs://{bucket_name}/data/{year}/{year}-*.parquet']
        );
        '''
    
    print(query)

    result = bigquery_query(
        query, gcp_credentials, 
    )
    return result


if __name__ == "__main__":

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-credentials")
    #gcs_bucket = GcsBucket.load("zoom-gcs")
    bq_dataset_name = 'ds_zoomcamp'
    bq_table_name = "bbg_top_100"

    #print(gcs_bucket)

    bucket_name = "prefect-de-course-zoomcamp"
    project_id = "dtc-de-course-375312"
    year = 2018

    #credentials= gcp_credentials_block.get_credentials_from_service_account(),

    #gs://prefect-de-course-zoomcamp/data/2018/2018-01.parquet

    #print(gcp_credentials_block)



    populate_bq(gcp_credentials_block, bucket_name, bq_dataset_name, bq_table_name, year)
