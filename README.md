# ETL pipeline with airflow

This is an ETL pipeline working with iss data and managing
the pipeline with apache airflow

### architecture
The program consists of two dags
1. data_processing_dag
   - This dag groups tasks that are responsible for Extraction from API.
   - Transformation to the designed format
2. data_insertion_dag
   - This dag groups two taks, one for creating a table in big query if it does not exits
   - second dag for uploading the data to bigquery

### Data Management

- ####  variables
Api keys, variables and important data to be passed between dags are using
airflow variables

- ####  xcoms
To make communications easier tasks inside of each dag use xcoms push and pull methods

- #### connections
Connection to bigquery is done by using Airflows connection so that user does not have to 
specify service account and other credentials