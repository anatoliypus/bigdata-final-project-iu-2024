DROP DATABASE IF EXISTS team29_projectdb CASCADE;

CREATE DATABASE team29_projectdb LOCATION "project/hive/warehouse";
USE team29_projectdb;

-- city table
CREATE EXTERNAL TABLE city STORED AS AVRO LOCATION 'project/warehouse/city'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/city.avsc');

-- dataset table
CREATE EXTERNAL TABLE dataset STORED AS AVRO LOCATION 'project/warehouse/dataset'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/dataset.avsc');

SELECT * FROM city LIMIT 50;
SELECT * FROM dataset LIMIT 50;

-- Create table for the dataset partitioned by the city_id
CREATE EXTERNAL TABLE dataset_part (
    date_time TIMESTAMP,
    max_temp INT,
    min_temp INT,
    total_snow DECIMAL(10, 5),
    sun_hour DECIMAL(10, 5),
    uv_index_1 INT,
    uv_index_2 INT,
    moon_illumunation INT,
    moonrise STRING,
    moonset STRING,
    sunrise STRING,
    sunset STRING,
    dew_point INT,
    feels_like INT,
    heat_index INT,
    wind_chill INT,
    wind_gust INT,
    cloudcover INT,
    humidity INT,
    precip DECIMAL(10, 5),
    pressure INT,
    temp INT,
    visibility INT,
    wind_dir INT,
    wind_speed INT
)
PARTITIONED BY (city_id INT)
STORED AS AVRO
LOCATION 'project/hive/warehouse/dataset_part'
TBLPROPERTIES ('avro.compress'='SNAPPY');

-- Change the configuratioins to allow dynamic partition
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Setting the Execution Engine to MapReduce
-- SET hive.execution.engine=mr;

--Insert our data to the partitioned table
INSERT INTO dataset_part partition (city_id) SELECT * FROM dataset;

-- Create table with bucketing
CREATE EXTERNAL TABLE dataset_part_buck (
    date_time TIMESTAMP,
    max_temp INT,
    min_temp INT,
    total_snow DECIMAL(10, 5),
    sun_hour DECIMAL(10, 5),
    uv_index_1 INT,
    uv_index_2 INT,
    moon_illumunation INT,
    moonrise STRING,
    moonset STRING,
    sunrise STRING,
    sunset STRING,
    dew_point INT,
    feels_like INT,
    heat_index INT,
    wind_chill INT,
    wind_gust INT,
    cloudcover INT,
    humidity INT,
    precip DECIMAL(10, 5),
    pressure INT,
    temp INT,
    visibility INT,
    wind_dir INT,
    wind_speed INT
)
PARTITIONED BY (city_id INT)
CLUSTERED BY (date_time) INTO 10 BUCKETS
STORED AS AVRO
LOCATION 'project/hive/warehouse/dataset_part_buck'
TBLPROPERTIES ('avro.compress'='SNAPPY');

--Insert our data to the partitioned table with bucketing
INSERT INTO dataset_part_buck partition (city_id) SELECT * FROM dataset;

-- Drop the original dataset table
-- The tables with partitions and buckets will be used in the next steps
DROP TABLE IF EXISTS dataset;