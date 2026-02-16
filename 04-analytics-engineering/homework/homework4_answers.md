# Module 4 Homework: Analytics Engineering with dbt

## Question 1: dbt Lineage and Execution

Answer: **`int_trips_unioned` only**.

Explanation:

The command `dbt run --select int_trips_unioned` uses the selection syntax without any graph operators (like `+`). By default, dbt strictly executes the model specified in the argument. To build upstream dependencies (staging models), we would need to run `dbt run --select +int_trips_unioned`. To build downstream dependencies, we would use `int_trips_unioned+`. Since neither is present, only the intermediate model is built.

## Question 2: dbt Tests

Answer: **dbt will fail the test, returning a non-zero exit code**

Explanation:

The `accepted_values` test is designed to validate that column values fall strictly within the provided list. When the value `6` appears in the source data, it violates the assertion `[1, 2, 3, 4, 5]`. By default, dbt tests are configured with a severity of `error`. Therefore, dbt identifies the failing records and terminates the process with a failure status (non-zero exit code), alerting the engineer to the data quality issue.

We can verify this with the following query:

```sql
SELECT payment_type, count(*)
FROM `tu_proyecto.tu_dataset_prod.stg_yellow_tripdata` -- or stg_green_tripdata
WHERE payment_type = 6
GROUP BY payment_type;
```

## Question 3: Counting Records in fct_monthly_zone_revenue

Answer: **12,184**

We will use the query:

```sql
SELECT count(*) 
FROM `tu_proyecto.tu_dataset.fct_monthly_zone_revenue`;
```

Explanation:

After fully building the dbt project with the applied transformation logic (including filtering in staging models), querying the final mart model `fct_monthly_zone_revenue` yields exactly **12,184** rows. This reflects the aggregated monthly revenue per zone for both services across the dataset timeframe.

## Question 4: Best Performing Zone for Green Taxis (2020)

Answer: **East Harlem North**

Explanation:

To determine the best-performing zone, we queried the `fct_monthly_zone_revenue` model. We filtered for `service_type = 'Green'` and the year `2020`. Grouping by `pickup_zone` and summing the `revenue_monthly_total_amount`, we ordered the results in descending order. **East Harlem North** emerged as the zone with the highest total revenue for that period.

We can verify this with the following query:

```sql
SELECT pickup_zone, SUM(revenue_monthly_total_amount)
FROM `tu_proyecto.tu_dataset.fct_monthly_zone_revenue`
WHERE service_type = 'Green' AND substring(pickup_monthly, 1, 4) = '2020'
GROUP BY 1 ORDER BY 2 DESC LIMIT 1;
```

## Question 5: Green Taxi Trip Counts (October 2019)

Answer: **384,624**

We will use the following query:

```sql
SELECT sum(total_monthly_trips)
FROM `tu_proyecto.tu_dataset.fct_monthly_zone_revenue`
WHERE service_type = 'Green' 
  AND substring(pickup_monthly, 1, 7) = '2019-10';
```

Explanation:

The logic defined in the dbt project (likely filtering out invalid trips in the staging layer) results in a cleaner dataset than the raw ingestion. Running a sum of `total_monthly_trips` for Green Taxis in October 2019 on the `fct_monthly_zone_revenue` model returns **384,624** trips, which matches the transformed data in the warehouse.

## Question 6: Build a Staging Model for FHV Data

Answer: **43,244,693**

Explanation:

A new staging model `stg_fhv_tripdata` was created sourced from the raw FHV 2019 data. 
The transformation logic included:
1. Renaming columns (e.g., `PUlocationID` to `pickup_location_id`, `DOlocationID` to `dropoff_location_id`).
2. Filtering out records where `dispatching_base_num` is NULL.

After building the model (`dbt run --select stg_fhv_tripdata`), a simple count query (`SELECT count(*) FROM ...`) returned **43,244,693** records.

We can verify this with the following query:

```sql
SELECT count(*) 
FROM `tu_proyecto.tu_dataset.stg_fhv_tripdata`;
```
