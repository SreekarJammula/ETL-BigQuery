import argparse

from google.cloud import bigquery


dataset_id = "demos"
dest_table_id=""


def construct_query():
    
    query="""select state,count(*) as num_stations from `bigquery-public-data.noaa_spc.wind_reports` group by(state) 
             order by (num_stations)DESC"""
    return query
   
  
  

def initial_query(query):
    
   
    client = bigquery.Client("durable-firefly-182023")
    
    job_config = bigquery.QueryJobConfig()
    job_config.allow_large_results = True
    
    dest_dataset_ref = client.dataset(dataset_id)
    dest_table_ref = dest_dataset_ref.table(dest_table_id)
    
    job_config.destination = dest_table_ref
    job_config.write_disposition = 'WRITE_TRUNCATE'

    query_job = client.query(query, job_config=job_config)
    


def export_data_to_gcs(dest_table_id,destination):
   
    
    bigquery_client = bigquery.Client("durable-firefly-182023")
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(dest_table_id)
    print(table_ref)

    job = bigquery_client.extract_table(table_ref, destination)
    job.result()  

    print('Exported {} to {}'.format( dest_table_id, destination))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
   
    parser.add_argument("destination",help="Enter the gcs path to store the file ")
    args = parser.parse_args()
    
  
    dest_table_id="wind_reports_output"
    
    query=construct_query()
    initial_query(query)
    export_data_to_gcs(dest_table_id, args.destination)


