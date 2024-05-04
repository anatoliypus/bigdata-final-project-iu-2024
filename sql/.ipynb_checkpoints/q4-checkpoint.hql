-- Query 4: Average Wind Speed by City
USE team29_projectdb;

-- Drop the existing results table if it exists
DROP TABLE IF EXISTS q4_results;

-- Create an external table to store the results
CREATE EXTERNAL TABLE q4_results (
    city_id INT,
    avg_wind_speed FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q4';

-- Calculate the average wind speed by city
INSERT INTO q4_results
SELECT 
    city_id,
    AVG(wind_speed) AS avg_wind_speed
FROM dataset_part_buck
GROUP BY city_id;

-- Display the results
SELECT * FROM q4_results;