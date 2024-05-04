-- Query 5: Pressure Distribution
USE team29_projectdb;

-- Drop the existing results table if it exists
DROP TABLE IF EXISTS q5_results;

-- Create an external table to store the results
CREATE EXTERNAL TABLE q5_results (
    pressure INT,
    count_pressure INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q5';

-- Calculate the distribution of pressure values
INSERT INTO q5_results
SELECT 
    pressure,
    COUNT(*) AS count_pressure
FROM dataset_part_buck
GROUP BY pressure;

-- Display the results
SELECT * FROM q5_results;
