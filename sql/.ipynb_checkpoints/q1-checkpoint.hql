-- Query 1: Average Temperature by City
USE team29_projectdb;

-- Drop the existing results table if it exists
DROP TABLE IF EXISTS q1_results;

-- Create an external table to store the results
CREATE EXTERNAL TABLE q1_results (
    city_id INT,
    avg_max_temp FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q1';

-- Calculate the average maximum temperature by city_id
INSERT INTO q1_results
SELECT 
    city_id,
    AVG(max_temp) AS avg_max_temp
FROM dataset_part_buck
GROUP BY city_id;

-- Display the results
SELECT * FROM q1_results;