## Module 4 Homework

### Question 2: 

To enable Analytics Engineers to dynamically control the date range, we should update the WHERE clause to use `var` with a fallback to `env_var` so that command line arguments take precedence over environment variables, which in turn take precedence over a default value.


### Question 3:

`dbt run --select models/staging/+`  excludes `dim_zones.sql` which is a dependency of `fct_taxi_monthly_zone_revenue.sql`


### Question 4:

Only the second statement is wrong since the `DBT_BIGQUERY_STAGING_DATASET` environment variable is optional because the macro falls back to `DBT_BIGQUERY_TARGET_DATASET` if `DBT_BIGQUERY_STAGING_DATASET` is not set.


### Question 5: Taxi Quarterly Revenue Growth

Create the following files to implement the solution:

1. [fct_taxi_trips_quarterly_revenue.sql](taxi_rides_ny/models/core/fct_taxi_trips_quarterly_revenue.sql) - This model will compute the quarterly revenues and the YoY growth.
2. [analyze_quarterly_growth.sql](taxi_rides_ny/models/core/analyze_quarterly_growth.sql) - This query will analyze the results and identify the best and worst quarters.

#### 1. `fct_taxi_trips_quarterly_revenue.sql`

```sql name=models/core/fct_taxi_trips_quarterly_revenue.sql
{{
    config(
        materialized='table'
    )
}}

-- Step 1: Extract the year, quarter, and service type from pickup_datetime
with temp as (
    SELECT 
        EXTRACT(YEAR FROM pickup_datetime) AS year, 
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter, 
        service_type, 
        total_amount
    FROM {{ ref('fact_trips') }}
),

-- Step 2: Group by service_type, year, and quarter to calculate total quarterly revenue
grouped as (
    SELECT 
        service_type, 
        year, 
        quarter, 
        SUM(total_amount) AS total_amount 
    FROM temp
    GROUP BY service_type, year, quarter
)

-- Step 3: Calculate YoY percentage change for each quarter
SELECT 
    service_type,
    year,
    quarter,
    total_amount,
    -- Get the total amount for the same quarter in the previous year
    LAG(total_amount) OVER (
        PARTITION BY service_type, quarter ORDER BY year
    ) AS prev_year_total_amount,
    -- Calculate YoY percentage change, avoid division by zero
    CASE 
        WHEN LAG(total_amount) OVER (
            PARTITION BY service_type, quarter ORDER BY year
        ) = 0 THEN NULL
        ELSE ROUND(
            (total_amount - LAG(total_amount) OVER (
                PARTITION BY service_type, quarter ORDER BY year
            )) / NULLIF(LAG(total_amount) OVER (
                PARTITION BY service_type, quarter ORDER BY year
            ), 0) * 100, 2
        )
    END AS yoy_percentage_change
FROM grouped
ORDER BY service_type, year, quarter;
```

#### 2. `analyze_quarterly_growth.sql`

```sql name=models/analyze_quarterly_growth.sql
-- Step 1: Filter results for the year 2020 and format year_quarter
with quarterly_growth as (
    SELECT
        service_type,
        year,
        CONCAT(year, '/Q', quarter) AS year_quarter,
        yoy_percentage_change
    FROM {{ ref('fct_taxi_trips_quarterly_revenue') }}
    WHERE year = 2020
)

-- Step 2: Select and order results by year on year percentage change
SELECT
    service_type,
    year_quarter,
    yoy_percentage_change
FROM quarterly_growth
ORDER BY service_type, yoy_percentage_change DESC;
```

### Question 6:

### Step 1

**Create a new model [fct_taxi_trips_monthly_fare_p95.sql](taxi_rides_ny/models/core/fct_taxi_trips_monthly_fare_p95.sql):**
   - This model will filter out invalid entries.
   - Compute the continuous percentiles (P97, P95, P90) of `fare_amount` partitioned by `service_type`, `year`, and `month`.


We first filter out the invalid entries and then calculate the continuous percentiles for the `fare_amount`.

```sql name=models/core/fct_taxi_trips_monthly_fare_p95.sql
{{
    config(
        materialized='table'
    )
}}

-- Step 1: Filter out invalid entries
with valid_trips as (
    SELECT 
        service_type, 
        EXTRACT(YEAR FROM pickup_datetime) AS year, 
        EXTRACT(MONTH FROM pickup_datetime) AS month, 
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE fare_amount > 0 
      AND trip_distance > 0 
      AND payment_type_description IN ('Cash', 'Credit Card')
),

-- Step 2: Compute continuous percentiles (P97, P95, P90)
percentiles as (
    SELECT 
        service_type,
        year,
        month,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS p97,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS p95,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS p90
    FROM valid_trips
    GROUP BY service_type, year, month
)

SELECT
    service_type,
    year,
    month,
    p97,
    p95,
    p90
FROM percentiles
ORDER BY service_type, year, month;
```

### Analysis Query

To analyze the values for Green Taxi and Yellow Taxi in April 2020, we filter the results via [analyze_monthly_percentiles.sql](taxi_rides_ny/models/core/analyze_monthly_percentiles.sql)

```sql name=models/core/analyze_monthly_percentiles.sql
-- Step 1: Filter results for April 2020
with monthly_percentiles as (
    SELECT
        service_type,
        year,
        month,
        p97,
        p95,
        p90
    FROM {{ ref('fct_taxi_trips_monthly_fare_p95') }}
    WHERE year = 2020 AND month = 4
)

-- Step 2: Select and order results
SELECT
    service_type,
    p97,
    p95,
    p90
FROM monthly_percentiles
ORDER BY service_type;
```

### Summary of Results

By running the above query, we get the continuous percentiles for April 2020 for both Green and Yellow taxis:

- **Green Taxi:**
  - P97: 55.0
  - P95: 45.0
  - P90: 26.5

- **Yellow Taxi:**
  - P97: 31.5
  - P95: 25.5
  - P90: 19.0


### Question 7:

Create the following files to implement the solution:

1. [stg_fhv_tripdata.sql](taxi_rides_ny/models/staging/stg_fhv_tripdata.sql) - Staging model for FHV Data (2019).
2. [dim_fhv_trips.sql](taxi_rides_ny/models/core/dim_fhv_trips.sql) - Core model for FHV Data.
3. [fct_fhv_monthly_zone_traveltime_p90.sql](taxi_rides_ny/models/core/fct_fhv_monthly_zone_traveltime_p90.sql) - Fact model to compute p90 trip_duration.
4. [analyze_longest_p90_travel_time.sql](taxi_rides_ny/models/core/analyze_longest_p90_travel_time.sql) - Analysis query to find the 2nd longest p90 trip_duration for specified pickup locations.

#### 1. Staging Model for FHV Data (2019): [stg_fhv_tripdata.sql](taxi_rides_ny/models/staging/stg_fhv_tripdata.sql)

```sql name=models/staging/stg_fhv_tripdata.sql
{{ config(materialized='view') }}

with tripdata as (
  select *
  from {{ source('staging', 'fhv_tripdata') }}
  where Dispatching_base_num is not null
)
select
    unique_row_id,
    Dispatching_base_num,
    cast(Pickup_datetime as timestamp) as pickup_datetime,
    cast(DropOff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOLocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    SR_Flag
from tripdata
```

#### 2. Core Model for FHV Data: [dim_fhv_trips.sql](taxi_rides_ny/models/core/dim_fhv_trips.sql)

```sql name=models/core/dim_fhv_trips.sql
{{
    config(
        materialized='table'
    )
}}

with tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    tripdata.unique_row_id,
    tripdata.Dispatching_base_num,
    tripdata.pickup_datetime,
    tripdata.dropoff_datetime,
    tripdata.pickup_locationid,
    tripdata.dropoff_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    tripdata.SR_Flag,
    EXTRACT(YEAR FROM pickup_datetime) AS year, 
    EXTRACT(MONTH FROM pickup_datetime) AS month
from tripdata
inner join dim_zones as pickup_zone
on tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on tripdata.dropoff_locationid = dropoff_zone.locationid
```

#### 3. Fact Model to Compute p90 Trip Duration: [fct_fhv_monthly_zone_traveltime_p90.sql](taxi_rides_ny/models/core/fct_fhv_monthly_zone_traveltime_p90.sql)

```sql name=models/core/fct_fhv_monthly_zone_traveltime_p90.sql
{{
    config(
        materialized='view'
    )
}}

with temp as (
    select
        unique_row_id,
        Dispatching_base_num,
        pickup_locationid,
        dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
        pickup_zone,
        dropoff_zone,
        year,
        month
    from {{ ref('dim_fhv_trips') }}
    where pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East') and year = 2019 and month = 11
)
select 
    year, 
    month,
    pickup_locationid,
    dropoff_locationid,
    pickup_zone,
    dropoff_zone,
    PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) as p90
from temp
order by
    pickup_zone, p90 desc
```

#### 4. Analysis Query: [analyze_longest_p90_travel_time.sql](taxi_rides_ny/models/core/analyze_longest_p90_travel_time.sql)

```sql name=models/core/analyze_longest_p90_travel_time.sql
-- Step 1: Filter results for November 2019
with november_2019 as (
    select
        pickup_zone,
        dropoff_zone,
        p90,
        ROW_NUMBER() OVER (PARTITION BY pickup_zone ORDER BY p90 DESC) as rn
    from {{ ref('fct_fhv_monthly_zone_traveltime_p90') }}
    where year = 2019 and month = 11
)

-- Step 2: Select the 2nd longest p90 trip duration for each specified pickup zone
select
    pickup_zone,
    dropoff_zone,
    p90
from november_2019
where rn = 2
order by pickup_zone;
```
