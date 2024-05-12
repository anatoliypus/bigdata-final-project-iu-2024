"""
Module for processing and preparing data using Spark ML tools
with specific transformations and scalings for a machine learning project.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import StandardScaler, MinMaxScaler, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, radians, sin, cos, year, month, dayofmonth, hour, minute

TEAM_ID = 29
WAREHOUSE_LOCATION = "project/hive/warehouse"

def create_spark_session(team_id, warehouse_location):
    """
    Creates and returns a SparkSession configured for Hive support.
    """
    return (SparkSession.builder
            .appName(f"{team_id} - Spark ML")
            .master("yarn")
            .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")
            .config("spark.sql.warehouse.dir", warehouse_location)
            .config("spark.sql.avro.compression.codec", "snappy")
            .enableHiveSupport()
            .getOrCreate())

def prepare_data(spark, table_name):
    """
    Load data from Hive, apply transformations, scale features, and save the prepared data.
    """
    df = spark.read.table(table_name)

    # Convert string columns to proper types
    df = df.withColumn("date_time", F.col("date_time").cast("timestamp"))
    df = df.withColumn("city_id", F.col("city_id").cast("int"))

    # Columns to be scaled using MinMaxScaler and StandardScaler
    minmax_columns = ["sun_hour", "uv_index_1", "uv_index_2", "moon_illumination",
                      "dew_point", "wind_gust", "cloudcover", "humidity",
                      "visibility", "wind_speed"]

    sc_columns = ["max_temp", "min_temp", "feels_like", "heat_index",
                  "wind_chill", "precip", "pressure", "temp"]

    # UDF for converting column type from vector to double type
    unlist = udf(lambda x: round(float(list(x)[0]), 3), DoubleType())

    # Applying transformations
    for columns, scaler in [(minmax_columns, MinMaxScaler()), (sc_columns, StandardScaler())]:
        for column in columns:
            assembler = VectorAssembler(inputCols=[column], outputCol=column + "_Vect")
            pipeline = Pipeline(stages=[assembler, scaler.setInputCol(column + "_Vect").setOutputCol(column + "_Scaled")])
            df = pipeline.fit(df).transform(df)
            df = df.withColumn(column + "_Scaled", unlist(column + "_Scaled")).drop(column + "_Vect")
            df = df.drop(column)

    # Additional transformations
    df = df.drop("sunrise", "sunset", "moonrise", "moonset")

    # Encode 'city_id' as one-hot vectors
    categ = df.select('city_id').distinct().rdd.flatMap(lambda x: x).collect()
    exprs = [F.when(F.col('city_id') == cat, 1).otherwise(0).alias(str(cat)) for cat in categ]
    df = df.select(exprs + df.columns)
    df = df.drop("city_id")

    # Save the preprocessed data to a new table
    output_table_name = "team29_projectdb.dataset_prepared_for_modeling"
    df.write.mode("overwrite").saveAsTable(output_table_name)

    print("Data preparation is done.")

if __name__ == "__main__":
    SPARK = create_spark_session(TEAM_ID, WAREHOUSE_LOCATION)
    TABLE_NAME = "team29_projectdb.dataset_part"
    prepare_data(SPARK, TABLE_NAME)