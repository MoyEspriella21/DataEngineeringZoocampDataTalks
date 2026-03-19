# Homework

In this homework, we'll practice streaming with Kafka (Redpanda) and PyFlink.

We use Redpanda, a drop-in replacement for Kafka. It implements the same protocol, so any Kafka client library works with it unchanged.

For this homework we will be using Green Taxi Trip data from October 2025:

## Setup

We'll use the same infrastructure from the workshop.

Follow the setup instructions: build the Docker image, start the services:

```Bash
cd 07-streaming/workshop/
docker compose build
docker compose up -d
```

This gives us:

- Redpanda (Kafka-compatible broker) on localhost:9092
- Flink Job Manager at http://localhost:8081
- Flink Task Manager
- PostgreSQL on localhost:5432 (user: postgres, password: postgres)

If you previously ran the workshop and have old containers/volumes, do a clean start:

```Bash
docker compose down -v
docker compose build
docker compose up -d
```

Note: the container names (like workshop-redpanda-1) assume the directory is called workshop. If you renamed it, adjust accordingly.

## Question 1. Redpanda version

Run rpk version inside the Redpanda container:

```Bash
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

Answer: **v25.3.9**

### Explanation

To find the version of Redpanda running inside the Docker container, we use the `docker exec` command to run the Redpanda Keeper (`rpk`) CLI tool. 

Assuming the Docker containers are already up and running, executing the following command in the terminal outputs the installed version:

```bash
docker exec -it workshop-redpanda-1 rpk version
```

## Question 2. Sending data to Redpanda

Create a topic called green-trips:

```Bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- lpep_pickup_datetime
- lpep_dropoff_datetime
- PULocationID
- DOLocationID
- passenger_count
- trip_distance
- tip_amount
- total_amount

Convert each row to a dictionary and send it to the green-trips topic. You'll need to handle the datetime columns - convert them to strings before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:

```Bash
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?
- 10 seconds
- 60 seconds
- 120 seconds
- 300 seconds

Answer: **10 seconds**

### Explanation

First, we create the topic in our Redpanda broker using the Redpanda Keeper (rpk) CLI tool:

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Next, we write a Python script to act as our producer. We load the Green Taxi dataset for October 2025 using pandas, filtering only the required columns. A crucial step before serializing to JSON is converting the datetime columns (lpep_pickup_datetime and lpep_dropoff_datetime) to string format, as standard JSON does not natively support datetime objects.

We then configure a KafkaProducer pointing to localhost:9092 with a JSON value serializer. We convert the pandas DataFrame to a list of dictionaries using df.to_dict(orient='records') for efficient iteration. 

Finally, we record the start time, iterate through the list to send each row to the green-trips topic, and call producer.flush() to ensure all messages in the local buffer are transmitted to the broker before recording the end time. 

Here is the code used to measure the execution:

```python
import pandas as pd
import json
from time import time
from kafka import KafkaProducer

url = "[https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet)"
columns = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 
    'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount'
]

df = pd.read_parquet(url, columns=columns)

df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'green-trips'
records = df.to_dict(orient='records')

t0 = time()

for row in records:
    producer.send(topic_name, value=row)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```
The entire loop and flush operation was completed in under 10 seconds.

## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the green-trips topic (set auto_offset_reset='earliest').

Count how many trips have a trip_distance greater than 5.0 kilometers.

How many trips have trip_distance > 5?
- 6506
- 7506
- 8506
- 9506

Answer: **8506**

### Explanation

To answer this, we need to create a Python consumer script that connects to our Redpanda broker and reads the messages published to the `green-trips` topic. 

Setting `auto_offset_reset='earliest'` ensures that the consumer reads from the very beginning of the topic, capturing the entirety of the dataset we just produced. We also set a `group_id` to manage the offsets for this specific reading task. 

Since streaming consumers naturally listen indefinitely for incoming data, we add the `consumer_timeout_ms=5000` parameter. This instructs the consumer to break the `for` loop if no new messages are received for 5 seconds, assuming the topic is fully consumed. Inside the loop, we access the deserialized JSON payload and increment a counter if the `trip_distance` is strictly greater than 5.0.

Here is the code used to perform the aggregation:

```python
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='distance-counter-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=5000
)

print("Reading messages...")
count = 0

for message in consumer:
    ride = message.value
    if float(ride.get('trip_distance', 0.0)) > 5.0:
        count += 1

print(f"Total trips with trip_distance > 5.0: {count}")
```

And we just execute our script:

```bash
uv run python consumer_green.py
```

# Part 2: PyFlink (Questions 4-6)

For the PyFlink questions, you'll adapt the workshop code to work with the green taxi data. The key differences from the workshop:

- Topic name: green-trips (instead of rides)
- Datetime columns use lpep_ prefix (instead of tpep_)
- You'll need to handle timestamps as strings (not epoch milliseconds)

You can convert string timestamps to Flink timestamps in your source DDL:

```Bash
lpep_pickup_datetime VARCHAR,
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Before running the Flink jobs, create the necessary PostgreSQL tables for your results.

Important notes for the Flink jobs:

- Place your job files in workshop/src/job/ - this directory is mounted into the Flink containers at /opt/src/job/
- Submit jobs with: docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/your_job.py
- The green-trips topic has 1 partition, so set parallelism to 1 in your Flink jobs (env.set_parallelism(1)). With higher parallelism, idle consumer subtasks prevent the watermark from advancing.
- Flink streaming jobs run continuously. Let the job run for a minute or two until results appear in PostgreSQL, then query the results. You can cancel the job from the Flink UI at http://localhost:8081
- If you sent data to the topic multiple times, delete and recreate the topic to avoid duplicates: docker exec -it workshop-redpanda-1 rpk topic delete green-trips

## Question 4. Tumbling window - pickup location

Create a Flink job that reads from green-trips and uses a 5-minute tumbling window to count trips per PULocationID.

Write the results to a PostgreSQL table with columns: window_start, PULocationID, num_trips.

After the job processes all data, query the results:

```SQL
SELECT PULocationID, num_trips
FROM <your_table>
ORDER BY num_trips DESC
LIMIT 3;
```

Which PULocationID had the most trips in a single 5-minute window?
- 42
- 74
- 75
- 166

Answer: **74**

### Explanation

To answer this, we need to build a PyFlink streaming job that aggregates data using a tumbling window. 

First, we create the destination table in PostgreSQL:

```sql
CREATE TABLE trip_counts (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
```

Next, we write the PyFlink job. The key adaptations for this specific task include parsing the string-based timestamps into Flink timestamps using TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss') and defining the WATERMARK on that computed column.

Crucially, because our green-trips Kafka topic only has 1 partition, we must enforce env.set_parallelism(1) in the Flink execution environment. If we leave the default parallelism, the idle subtasks will never receive data, their individual watermarks will not advance, and the global watermark will be held back indefinitely, resulting in no data being written to PostgreSQL.

Here is the PyFlink job code (q4_tumbling_window.py):

```Python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_source_table(t_env):
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            PULocationID INT,
            lpep_pickup_datetime VARCHAR,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'q4-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)
    return "green_trips"

def create_sink_table(t_env):
    t_env.execute_sql("""
        CREATE TABLE trip_counts (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trip_counts',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    return "trip_counts"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_source_table(t_env)
    sink_table = create_sink_table(t_env)

    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT 
            window_start, 
            PULocationID, 
            COUNT(*) as num_trips
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, PULocationID
    """).wait()

if __name__ == '__main__':
    main()
```

After submitting the job using docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_window.py and letting it process the stream, querying the PostgreSQL table reveals that PULocationID 74 (East Harlem North) had the highest number of trips in a single 5-minute window.

## Question 5. Session window - longest streak

Create another Flink job that uses a session window with a 5-minute gap on PULocationID, using lpep_pickup_datetime as the event time with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other. When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the PULocationID with the longest session (most trips in a single session).

How many trips were in the longest session?
- 12
- 31
- 51
- 81

Answer: **81**

### Explanation

A Session Window is a stream processing function that groups events dynamically based on periods of activity. Unlike fixed windows, a session window has no predetermined length. It continues to expand as long as events arrive within a specified time gap (5 minutes in this case). If the gap period elapses without any new events for a specific key (PULocationID), the window closes. A Watermark is a time tolerance threshold telling Flink how long to wait for late-arriving events before processing the window.

First, we create the destination table in PostgreSQL. We must include both window_start and window_end in the primary key because session windows have variable lengths, making the combination of start, end, and location unique.

```sql
CREATE TABLE trip_sessions (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);
```

Next, we write the PyFlink job:

```Python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_source_table(t_env):
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            PULocationID INT,
            lpep_pickup_datetime VARCHAR,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'q5-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)
    return "green_trips"

def create_sink_table(t_env):
    t_env.execute_sql("""
        CREATE TABLE trip_sessions (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trip_sessions',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    return "trip_sessions"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_source_table(t_env)
    sink_table = create_sink_table(t_env)

    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT 
            window_start, 
            window_end,
            PULocationID, 
            COUNT(*) as num_trips
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID
    """).wait()

if __name__ == '__main__':
    main()
```

The SQL query uses the SESSION function instead of TUMBLE. It groups by window_start, window_end, and PULocationID. We maintain env.set_parallelism(1) because the Redpanda topic only has a single partition.

After submitting the job and querying the trip_sessions table in PostgreSQL, sorting by num_trips in descending order reveals the longest continuous session contained 81 trips.

## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the total tip_amount per hour (across all locations).

Which hour had the highest total tip amount?
- 2025-10-01 18:00:00
- 2025-10-16 18:00:00
- 2025-10-22 08:00:00
- 2025-10-30 16:00:00

Answer: **2025-10-16 18:00:00**

### Explanation

For this final task, we need to compute an aggregation across the entire dataset without grouping by location. We use a 1-hour tumbling window to sum the tip_amount for all green taxi trips occurring within each hour.

First, we create a simplified destination table in PostgreSQL. Since we are aggregating across all locations globally, the primary key is solely the window_start.

```sql
CREATE TABLE hourly_tips (
    window_start TIMESTAMP,
    total_tips DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);
```

Next, we write the PyFlink job. The SQL logic changes to remove PULocationID from both the SELECT clause and the GROUP BY clause. We use the SUM(tip_amount) aggregate function to calculate the total tips per 1-hour tumbling window.

Here is the Flink job code (q6_tumbling_tips.py):

```Python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_source_table(t_env):
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            tip_amount DOUBLE,
            lpep_pickup_datetime VARCHAR,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'q6-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)
    return "green_trips"

def create_sink_table(t_env):
    t_env.execute_sql("""
        CREATE TABLE hourly_tips (
            window_start TIMESTAMP(3),
            total_tips DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'hourly_tips',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    return "hourly_tips"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_source_table(t_env)
    sink_table = create_sink_table(t_env)

    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT 
            window_start, 
            SUM(tip_amount) as total_tips
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
        )
        GROUP BY window_start
    """).wait()

if __name__ == '__main__':
    main()
```

After submitting the job and letting the watermark advance through the historical data, we query the PostgreSQL table:

```SQL
SELECT window_start, total_tips
FROM hourly_tips
ORDER BY total_tips DESC
LIMIT 1;
```
