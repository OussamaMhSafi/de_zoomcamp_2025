### Question 3

```sql
SELECT
    SUM(CASE WHEN Trip_distance <= 1 THEN 1 ELSE 0 END) AS "Up to 1 mile",
    SUM(CASE WHEN Trip_distance > 1 AND Trip_distance <= 3 THEN 1 ELSE 0 END) AS "1 to 3 miles",
    SUM(CASE WHEN Trip_distance > 3 AND Trip_distance <= 7 THEN 1 ELSE 0 END) AS "3 to 7 miles",
    SUM(CASE WHEN Trip_distance > 7 AND Trip_distance <= 10 THEN 1 ELSE 0 END) AS "7 to 10 miles",
    SUM(CASE WHEN Trip_distance > 10 THEN 1 ELSE 0 END) AS "Over 10 miles"
FROM
    green_tripdata_2019_10
```


### Question 4

```sql
WITH daily_longest_trip AS (
    SELECT
        DATE(lpep_pickup_datetime) AS pickup_day,
        MAX(Trip_distance) AS longest_trip_distance
    FROM
        green_tripdata_2019_10
    GROUP BY
        DATE(lpep_pickup_datetime)
)
SELECT
    pickup_day
FROM
    daily_longest_trip
WHERE
    longest_trip_distance = (SELECT MAX(longest_trip_distance) FROM daily_longest_trip);
```

### Question 5

```sql
SELECT
    tz."Zone" AS pickup_zone,
    SUM(gt."total_amount") AS total_revenue
FROM
    green_tripdata_2019_10 gt
JOIN
    taxi_zones tz
ON
    gt."PULocationID" = tz."LocationID"
WHERE
    DATE(gt."lpep_pickup_datetime") = '2019-10-18'
GROUP BY
    tz."Zone"
HAVING
    SUM(gt."total_amount") > 13000
ORDER BY
    total_revenue DESC
LIMIT 3;
```

### Question 6

```sql
WITH trips_with_pickup_zone AS (
    SELECT
        gt."DOLocationID",
        tz_pickup."Zone" AS pickup_zone,
        tz_dropoff."Zone" AS dropoff_zone,
        gt."tip_amount"
    FROM
        green_tripdata_2019_10 gt
    JOIN
        taxi_zones tz_pickup ON gt."PULocationID" = tz_pickup."LocationID"
    JOIN
        taxi_zones tz_dropoff ON gt."DOLocationID" = tz_dropoff."LocationID"
    WHERE
        tz_pickup."Zone" = 'East Harlem North'
)
SELECT
    dropoff_zone,
    MAX(tip_amount) AS largest_tip
FROM
    trips_with_pickup_zone
GROUP BY
    dropoff_zone
ORDER BY
    largest_tip DESC
LIMIT 1;
```

