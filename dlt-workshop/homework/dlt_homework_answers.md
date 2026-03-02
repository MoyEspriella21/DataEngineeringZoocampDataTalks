# dlt Workshop Homework: NYC Taxi Pipeline

Overview

This document contains the workflow and solutions for the "Build Your Own dlt Pipeline" homework, where we extracted NYC Yellow Taxi trip data from a custom paginated API and loaded it into a local DuckDB instance.

## Question 1: What is the start date and end date of the dataset?

Answer: **2009-06-01 to 2009-07-01**.

### Explanation & Process

To find the correct timeframe of the dataset, I first needed to build the data ingestion pipeline using `dlt`. Since the dataset was provided via a custom paginated REST API, I configured a `dlt.resource` to fetch data continuously until an empty page was returned. 

Here is the core logic used for the extraction and loading process:

```
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name="yellow_taxi_trips", write_disposition="replace")
def custom_api_source():
    client = RESTClient(
        base_url="<API_BASE_URL>",
        paginator=PageNumberPaginator(base_page=1, total_path=None)
    )

    for page in client.paginate("<API_ENDPOINT>"):
        yield page

# Define and run the pipeline
pipeline = dlt.pipeline(
    pipeline_name="custom_api_pipeline",
    destination="duckdb",
    dataset_name="api_dataset"
)
load_info = pipeline.run(custom_api_source())
```

Once the pipeline successfully loaded the data into DuckDB, I connected to the local .duckdb database and executed an aggregation query to find the earliest and latest pickup dates in the dataset.

```
import duckdb

conn = duckdb.connect("custom_api_pipeline.duckdb")

query = """
SELECT 
    MIN(pickup_datetime_column) AS start_date, 
    MAX(pickup_datetime_column) AS end_date 
FROM api_dataset.yellow_taxi_trips
"""

print(conn.sql(query))
```

This query evaluated the entire loaded dataset and provided the exact bounds, matching the option selected above.

## Question 2: What proportion of trips are paid with credit card?

Answer: **26.66%**.

### Explanation

To calculate the proportion of trips paid with a credit card, I queried the loaded DuckDB database. According to the standard NYC TLC data dictionary, the `payment_type` column uses the value `1` to denote credit card transactions. I used a `CASE` statement to count these specific instances and divided them by the total number of records.

```
import duckdb

conn = duckdb.connect("taxi_pipeline.duckdb")

query_q2 = """
SELECT 
    ROUND((SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS credit_card_percentage
FROM api_dataset.yellow_taxi_trips
"""

print(conn.sql(query_q2))
```

## Question 3: What is the total amount of money generated in tips?

Answer: **$6,063.41**.

### Explanation

To find the total amount of tips, I ran a simple aggregation query using DuckDB to sum the tip_amount column across the entire dataset loaded by our dlt pipeline.

```
query_q3 = """
SELECT 
    SUM(tip_amount) AS total_tips_amount
FROM api_dataset.yellow_taxi_trips
"""

print(conn.sql(query_q3))
```
