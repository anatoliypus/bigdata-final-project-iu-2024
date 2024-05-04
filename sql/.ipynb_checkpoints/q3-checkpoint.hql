-- Query 3: UV Index Distribution
USE team29_projectdb;

-- Drop the existing results table if it exists
DROP TABLE IF EXISTS q3_results;

-- Create an external table to store the results
CREATE EXTERNAL TABLE q3_results (
    uv_index INT,
    count_uv_index INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q3';

-- Calculate the distribution of UV index
INSERT INTO q3_results
SELECT 
    uv_index_1 AS uv_index,
    COUNT(*) AS count_uv_index
FROM dataset_part_buck
GROUP BY uv_index_1
UNION ALL
SELECT 
    uv_index_2 AS uv_index,
    COUNT(*) AS count_uv_index
FROM dataset_part_buck
GROUP BY uv_index_2;

-- Display the results
SELECT * FROM q3_results;