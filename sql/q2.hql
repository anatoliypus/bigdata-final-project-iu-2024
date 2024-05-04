-- Query 2: Total Snowfall by Date
USE team29_projectdb;

-- Drop the existing results table if it exists
DROP TABLE IF EXISTS q2_results;

-- Create an external table to store the results
CREATE EXTERNAL TABLE q2_results (
    date_time STRING,
    total_snow DECIMAL(10, 5)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q2';

-- Calculate the total snowfall by date
INSERT INTO q2_results
SELECT 
    date_time,
    SUM(total_snow) AS total_snow
FROM dataset_part_buck
GROUP BY date_time;

-- Display the results
SELECT * FROM q2_results;