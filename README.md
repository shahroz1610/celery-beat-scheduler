# Celery Beat Scheduler
  Scheduler to migrate chunks of data at regular intervals from csv to MySQL.

## About the service
 There is a lot of data in CSV, you want to migrate the data from CSV to another DB (I have used MySQL here)
 We have a CSV with lots of data, user can create a schedule by provding the details in a dictionary.
 
 #### Sample dictionary
``{
"schedule_id": "001", // a unique id for a schedule.
"query": {"country": "USA"}, //dict: query to filter data
"chunk_size": 10, //integer: number of documents to pick at a time
"frequency": 3600, //integer: frequency in seconds
}``

The scheduler will schedule a job. The job will run the query on the `CSV` and pick up the first chunk of documents.
It will then insert the data-chunk in MySQL in a table with the same name as the schedule_id.
At the next schedule, the next chunk of data will be inserted


## To get the service running:
   1. ``` pip install -r requirements.txt ```
   2. Create a new .env file and add
      - MYSQL_CONN_STR = your connection string
      - REDIS_CONN_STR = your connecion string
   3. Open your terminal and run these commands 
      - To start worker       ``` celery -A scheduler worker --loglevel=INFO ```
      - To start the schduler ``` celery -A beat beat ```
