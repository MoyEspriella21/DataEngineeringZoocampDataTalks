# Question 1

To see the version of pip:

```bash
docker run -it --entrypoint=bash python:3.13
pip --version
```

# Question 3

For the trip_distance of less than or equal to 1 mile:

```sql
SELECT count(*) 
FROM green_taxi_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

# Question 4

Pick up day with the longest trip distance:

```sql
SELECT
    lpep_pickup_datetime,
    trip_distance
FROM
    green_taxi_trips
WHERE
    trip_distance < 100
ORDER BY
    trip_distance DESC
LIMIT 1;
```

# Question 5

Pickup zone with the largest total_amount:

```sql
SELECT
    z."Zone",
    SUM(t.total_amount) AS total_dinero
FROM
    green_taxi_trips t
JOIN
    zones z ON t."PULocationID" = z."LocationID"
WHERE
    t.lpep_pickup_datetime >= '2025-11-18' 
    AND t.lpep_pickup_datetime < '2025-11-19'
GROUP BY
    z."Zone"
ORDER BY
    total_dinero DESC
LIMIT 1;
```

# Question 6

Drop off zone with the largest tip for passenger picked up in "East Harlem North" in November 2025:

```sql
SELECT
    z_drop."Zone" AS zona_destino,
    t.tip_amount AS propina
FROM
    green_taxi_trips t
JOIN
    zones z_pick ON t."PULocationID" = z_pick."LocationID"
JOIN
    zones z_drop ON t."DOLocationID" = z_drop."LocationID"
WHERE
    z_pick."Zone" = 'East Harlem North'
    AND t.lpep_pickup_datetime >= '2025-11-01'
    AND t.lpep_pickup_datetime < '2025-12-01'
ORDER BY
    t.tip_amount DESC
LIMIT 1;
```
