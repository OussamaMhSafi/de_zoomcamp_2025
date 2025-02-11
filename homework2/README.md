## Module 2 Homework


### Question 3

```sql
SELECT COUNT(*)
FROM `ny-taxi-468414.kestra_homework.yellow_tripdata`
WHERE 
  EXTRACT(YEAR FROM tpep_pickup_datetime) = 2020
```

### Question 4

```sql
SELECT COUNT(*)
FROM `ny-taxi-468414.kestra_homework.green_tripdata`
WHERE 
  EXTRACT(YEAR FROM lpep_pickup_datetime) = 2020
```

### Question 5

```sql
SELECT COUNT(*)
FROM `ny-taxi-468414.kestra_homework.yellow_tripdata_2021_03`
```

### Question 6

```yaml
triggers:
  - id: example_of_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: "America/New_York"
```



