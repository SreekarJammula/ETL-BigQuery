ETL on Large datasets using Google BigQuery

  BigQuery is a very sophisticated data warehouse which helps us to easily perform ETL operations. Being a serverless tool, it helps in analyzing huge data sets within a very small amount of time and also allows us to connect with several other services within and outside GCP to ingest, consume and output the data. 
The main premise of this blog is to discuss a project that processes   huge amounts of weather data provided by NOAA and try to find out the hottest, coldest and windiest states in the United States. This project also aims at connecting to google cloud storage to output the queried data in the form of CSV files. This is helpful in cases when a federated dataset of decade wise data is required to find the evolution of weather patterns.

The dataset:
This project uses two different weather data sets provided by NOAA(National Oceanic and Atmospheric Administration). These data sets come as a standard public datasets in BigQuery.
NOAA_GSOD DATASET: This is a huge dataset of several meteorological factors such as maximum temperature, minimum temperature, precipitation etc., recorded at several weather stations across the world. 
The data is arranged in year wise tables(starting from year 1929 to 2017) wherein the factors for each weather station for every  day in that year are recorded(example table names:gsod2017,gsod2016 etc.,) .There is also another table  called ‘stations’ which consists the information about the stations.
NOAA_SPC DATASET: This dataset consists of the wind_reports table that has the information about the windspeeds across the United States along with their timestamps.
Architecture:  
The  Architecture can use three different services on GCP. A scheduler engine(i.e., a compute engine) can be designed to automatically design and run the python scripts which have the queries. These are then processed using BigQuery service and then the results are stored in GCS

Querying the data:
In order to query the data, I chose the Python API for BigQuery to develop python scripts to query the data and then export the results into the google storage buckets as csv files. The main objective of choosing the python api is because it provides some powerful functions which make the coding easy and also gives us to write a persistent query which can be changed easily.
I have written three scripts and given below is a description of their working and usage: 
Query_hottest_state.py: This contains the query to find the hottest state in the united states, specifically in the year 2017. This query is designed such that it returns the number of weather stations reporting  a daily mean temperature greater than 80F(which is considered a general cut off temperature that defines the weather to be hot)  grouped by states in the United states and displays the results in a descending order. It also has a function to export the queried data to GCS 

Usage:  python query_hottest_state.py <input table name> gs://<bucket-name>/<filename.csv>
The script takes two parameters, the input table (example: gsod2017) and the output file name in the google storage bucket as indicated. 
A more advance usage can be to use wildcards for tables  such as “gsod20*”so that the decade wise data can be analysed. It is always recommended to create a new bucket to store the data in the google cloud storage. Internally, the script queries the input table and stores the results into a pre-created table in google bigquery(which is referred as dest_table_id in the script). Then it calls a function to write this table into google storage.

Results: This script provides the following results indicating that TEXAS, CALIFORNIA and FLORIDA are the top three hottest states.  

Query_coldest_state.py: The usage and the functioning of this script is same as above except that it outputs the number of station which report a temperature of less than 10F grouped by state.
 Results:
 
Query_windiest_state.py: This script queries the wind_reports table in the noaa_spc dataset and outputs the states with highest number of winds reported in a descending order. This table consists of the incidents between 2015 to 2017
Usage: python query_windiest_state.py gs://<bucket-name/<filename.csv>
Results:
 


Scheduling the queries: 
Bigquery doesn’t essentially have a functionality that lets us to schedule the queries to run automatically. This can be done using third party packages such as CRON or AIRFLOW. Also, one can make use of recently introduced Google Cloud Functions or develop the programs in Google  Appscript which is a Javascript Api developed by Google.
Challenges: 
The major challenge I faced was to get myself acquainted to the python Api. BigQuery doesn’t allow the queried data to be exported to GCS without it being stored in a table and one major challenge I faced was creating the tables using the python API. Hence, I opted to manually create the tables using Web UI and then wrote the script to store the query results in these tables. Also, we cannot validate the query before we can execute it as possible in the Web UI. This maybe very useful in case of complex queries processing huge data.

Additional Reading: 
This project has given me a better understanding of Google BigQuery and how it helps to solve several data analysis problems. BigQuery uses a columnar storage format and scales automatically. Thus, by writing efficient queries which limit the number of columns being accessed, we can get the results within seconds. Considering the serverless architecture, the pricing can be said to be very reasonable. The customer is charged for storing the data and for the amount of data processed. 


