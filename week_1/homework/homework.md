# Question 2. Understanding docker first run
    docker run -it --entrypoint=bash python:3.9
    pip list

# Question 3. Count records
    SELECT CAST(lpep_pickup_datetime as DATE), count(1)
    FROM "yellow_taxi_data_2019"
    where CAST(lpep_pickup_datetime as DATE) = '2019-01-15' and CAST(lpep_dropoff_datetime as DATE) = '2019-01-15' 
    GROUP BY CAST(lpep_pickup_datetime as DATE)

# Question 4. Largest trip for each day
    SELECT CAST(lpep_pickup_datetime as DATE), max(trip_distance)
    FROM "yellow_taxi_data_2019"
    GROUP BY CAST(lpep_pickup_datetime as DATE)
    ORDER BY max(trip_distance) desc

# Question 5. The number of passengers
    SELECT passenger_count, count(1)
    FROM "yellow_taxi_data_2019"
    where (passenger_count = 2 or passenger_count = 3) and CAST(lpep_pickup_datetime as DATE) = '2019-01-01'
    GROUP BY passenger_count

# Question 6. Largest tip
    SELECT zpu."Zone", zdo."Zone", max(tip_amount)
    FROM yellow_taxi_data_2019 d
    LEFT JOIN yellow_taxi_data_zones zpu
    ON d."PULocationID" = zpu."LocationID"
    LEFT JOIN yellow_taxi_data_zones zdo
    ON d."DOLocationID" = zdo."LocationID"
    WHERE zpu."Zone" = 'Astoria'
    GROUP BY zpu."Zone", zdo."Zone"
    ORDER BY max(tip_amount) desc