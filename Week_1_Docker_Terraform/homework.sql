-- Question 3. Count records
-- How many taxi trips were totally made on September 18th 2019?
-- Tip: started and finished on 2019-09-18.
-- Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.
SELECT 
    COUNT(*) 
FROM 
    green_taxi_data
WHERE  
    lpep_pickup_datetime >= '2019-09-18 00:00:00'
AND  
    lpep_dropoff_datetime < ('2019-09-19 00:00:00');

-- Question 4. Largest trip for each day
-- Which was the pick up day with
SELECT 
	DATE(lpep_pickup_datetime),
	SUM(trip_distance)
 FROM 
	green_taxi_data 
GROUP BY 
	DATE(lpep_pickup_datetime) 
ORDER BY 
	SUM(trip_distance) DESC;

-- Question 5. The number of passengers
-- Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown
-- Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?


CREATE TABLE homework AS
    SELECT
	    lpep_pickup_datetime,
	    total_amount,
	    zpu."Borough" AS "pickup_loc"
    FROM 
	    green_taxi_trips t,
	    zones zpu
    WHERE
	    t."PULocationID" = zpu."LocationID" ;



SELECT
    pickup_loc,
    SUM(total_amount) AS total_amount_sum
FROM
    homework
WHERE
    lpep_pickup_datetime >= '2019-09-18 00:00:00'
    AND lpep_pickup_datetime < ('2019-09-19 00:00:00')
    AND pickup_loc != 'Unknown'
GROUP BY
    pickup_loc
HAVING
    SUM(total_amount) > 50000
ORDER BY
    total_amount_sum DESC

-- Question 6. Largest tip
-- For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
-- Note: it's not a typo, it's tip , not trip

CREATE TABLE homework2 AS
SELECT
	tip_amount,
	zpu."Zone" AS "pickup_loc",
	zdo."Zone" AS "dropoff_loc"
FROM 
	green_taxi_trips t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID" AND
	t."DOLocationID" = zdo."LocationID"



SELECT
	MAX(tip_amount) AS tip,
	dropoff_loc
FROM 
	homework2
WHERE
	pickup_loc ='Astoria'
GROUP BY 
	dropoff_loc
ORDER BY
	tip DESC;