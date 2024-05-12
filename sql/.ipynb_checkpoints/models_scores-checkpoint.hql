USE team29_projectdb;

-- Drop the existing results table if it exists
DROP TABLE IF EXISTS results_comparison;

CREATE EXTERNAL TABLE results_comparison (
    target STRING,
    `RMSE (GBT)` DOUBLE,
    `R2 (GBT)` DOUBLE,
    `RMSE (RF)` DOUBLE,
    `R2 (RF)` DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/hive/warehouse/models_results'
TBLPROPERTIES ("skip.header.line.count"="1");