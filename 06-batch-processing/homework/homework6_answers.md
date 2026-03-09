# Module 6 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2025-11 data from the official website:

```
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

## Question 1: Install Spark and PySpark

* Install Spark
* Run PySpark
* Create a local spark session
* Execute `spark.version`.

What's the output?

**Answer:**

Following the updated guidelines for the 2026 cohort, the setup process for Spark 4.x on macOS has been greatly simplified. Since PySpark 4.x bundles its own Spark distribution, there is no longer a need to manually download Spark binaries or set up the `SPARK_HOME` environment variable.

I installed Java 17 via Homebrew and used `uv` (an extremely fast Python package manager) to handle the dependencies:

```bash
# Install Java 17
brew install openjdk@17

# Initialize uv and add pyspark
uv init
uv add pyspark
```

After setting up the environment, I created a local Spark session to verify the installation:

```python
import pyspark
from pyspark.sql import SparkSession

# Initialize a local Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# Check the Spark version
print(f"Spark version: {spark.version}")
```

Output: **'4.0.1'**

The output confirms that Spark 4.x is successfully installed and running locally.

## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.
Repartition the Dataframe to 4 partitions and save it to parquet.
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? 

First, I downloaded the requested dataset using `wget`:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

Then, I used PySpark to read the file, repartition the dataframe into exactly 4 partitions, and write the output to a new directory. Repartitioning is a crucial operation in Spark to optimize parallel processing by distributing data evenly across executors.

```python
import pyspark
from pyspark.sql import SparkSession

# Initialize session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('Zoomcamp_Q2') \
    .getOrCreate()

# Read the original parquet file
df = spark.read.parquet('yellow_tripdata_2025-11.parquet')

# Repartition and save
output_path = 'data/pq/yellow/2025/11/'
df.repartition(4).write.parquet(output_path, mode='overwrite')
```

After the job completed, I checked the size of the generated `.parquet` files inside the output directory using the terminal:

```bash
ls -lh data/pq/yellow/2025/11/

```

Answer: **25MB**.

## Question 3: Count records

How many taxi trips were there on the 15th of November?
Consider only trips that started on the 15th of November.

Answer: **162,604**.

### Methodology

To find the total number of trips that started on a specific day, I needed to filter the dataframe based on the pickup datetime column (`tpep_pickup_datetime`). 

Since the column contains timestamp data (both date and time), I imported PySpark SQL functions to cast the timestamp to a simple date format using `F.to_date()`. This ensures that trips occurring at any time during that specific day are correctly grouped and counted.

```python
from pyspark.sql import functions as F

# Filter the dataframe for trips starting exactly on 2025-11-15 and count them
trips_nov_15 = df_trips.filter(
    F.to_date(df_trips.tpep_pickup_datetime) == '2025-11-15'
).count()

print(f"Total trips on November 15, 2025: {trips_nov_15}")
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

Answer: **90.6**

### Methodology

To find the longest trip, I needed to calculate the time difference between the dropoff and pickup datetime columns. Since Spark handles these as Timestamps, the most robust mathematical approach is to convert them into Unix timestamps (seconds since the Unix epoch), subtract the pickup time from the dropoff time, and divide the result by 3600 to convert seconds into hours.

I solved this using PySpark's DataFrame API:

```python
from pyspark.sql import functions as F

# Calculate duration in hours
df_with_duration = df_trips.withColumn(
    'duration_hours', 
    (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 3600
)

# Get the maximum duration
longest_trip = df_with_duration.select(F.max('duration_hours')).first()[0]
print(f"Longest trip in hours: {longest_trip}")
```

## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?
* 80
* 443
* 4040
* 8080

Answer: **4040**

When a `SparkSession` is initialized, Apache Spark automatically launches a Web UI to monitor and inspect the application's execution (including Jobs, Stages, Tasks, memory usage, and the DAG). 

By default, this application dashboard is hosted on port **4040**. (Note: If port 4040 is already in use by another active Spark context, Spark will increment the port number to 4041, 4042, and so on).

We can easily verify the exact URL of your active session programmatically by running:

```python
print(spark.sparkContext.uiWebUrl)
```

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark.
Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

Answer: **Governor's Island/Ellis Island/Liberty Island** or **Arden Heights**.

To identify the least frequent pickup zone, I needed to join the main trips dataset with the provided zone lookup CSV. As suggested by the instructions, I used Spark SQL to perform the aggregation and join.

First, I downloaded the lookup table into my raw data directory:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Next, I read the CSV into a Spark DataFrame, created temporary views for both datasets, and executed an SQL query to count the pickups per zone, ordering the results in ascending order to find the minimum:

```python
# Load the zone lookup data
df_zones = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('/data/homework/raw/taxi_zone_lookup.csv')

# Create temporary views
df_zones.createOrReplaceTempView("zones")
df_trips.createOrReplaceTempView("trips")

# Query to find the least frequent pickup location zone
spark.sql("""
    SELECT 
        z.Zone, 
        COUNT(1) AS pickup_count
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY pickup_count ASC
    LIMIT 1
""").show(truncate=False)

```
