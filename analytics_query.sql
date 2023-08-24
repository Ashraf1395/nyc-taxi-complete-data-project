
'''1. Total Trips Per Year:'''

SELECT EXTRACT(YEAR FROM pickup_datetime) AS year, COUNT(*) AS total_trips
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
GROUP BY year
ORDER BY year;

'''2. Average Trip Duration by Passenger Count:'''

SELECT passenger_count, AVG(trip_duration) AS avg_trip_duration
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
GROUP BY passenger_count
ORDER BY passenger_count;

'''3. Popular Pickup and Dropoff Locations:'''

SELECT pickup_location, COUNT(*) AS pickup_count
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
GROUP BY pickup_location
ORDER BY pickup_count DESC
LIMIT 10;

SELECT dropoff_location, COUNT(*) AS dropoff_count
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
GROUP BY dropoff_location
ORDER BY dropoff_count DESC
LIMIT 10;

'''4. Average Fare Amount by Payment Type:'''

SELECT payment_type, AVG(fare_amount) AS avg_fare_amount
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
GROUP BY payment_type
ORDER BY avg_fare_amount DESC;

'''5. Busiest Days of the Week for Trips:'''

SELECT pickup_weekday, COUNT(*) AS total_trips
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
GROUP BY pickup_weekday
ORDER BY pickup_weekday;

'''6. Average Tip Amount by Rate Code:'''

SELECT rate_name, AVG(tip_amount) AS avg_tip_amount
FROM `nyc_yellow_taxi.data_warehouse.fact_table`
JOIN `nyc_yellow_taxi.data_warehouse.dim_rate` ON fact_table.rate_id = dim_rate.rate_id
GROUP BY rate_name
ORDER BY avg_tip_amount DESC;
