-- Query 2: Average Sun Hours by City
USE team29_projectdb;

-- Drop the existing results table if it exists
DROP TABLE IF EXISTS q2_results;

-- Create an external table to store the results
CREATE EXTERNAL TABLE q2_results (
    city_id INT,
    avg_sun_hours FLOAT          
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q2';

-- Calculate the average sun hours by city_id
INSERT INTO q2_results
SELECT 
    city_id,
    AVG(sun_hour) AS avg_sun_hours   
FROM dataset_part_buck
GROUP BY city_id;

-- Display the results
SELECT * FROM q2_results;
