from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, sin, cos, radians
from pyspark.ml.feature import StandardScaler, MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from tqdm import tqdm
import pyspark.sql.functions as F 


team = 29

# location of your Hive database in HDFS
warehouse = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName("{} - spark ML".format(team))\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()


table_name = "team29_projectdb.dataset_part"
df = spark.read.table(table_name)





# Convert string columns to timestamp
df = df.withColumn("date_time", df["date_time"].cast("timestamp"))

# df = df.withColumn( "sunrise", df[ "sunrise"].cast("int"))
# df = df.withColumn( "sunset", df[ "sunset"].cast("int"))
# df = df.withColumn("moonrise", df["moonrise"].cast("int"))
df = df.withColumn("city_id", df["city_id"].cast("int"))






# Define columns for scaling
sc_columns = [
    "max_temp",
    "min_temp",
    "feels_like",
    "heat_index",
    "wind_chill",
    "precip",
    "pressure",
    "temp"
]

minmax_columns = [
    "sun_hour",
    "uv_index_1",
    "uv_index_2",
    "moon_illumunation",
    "dew_point",
    "wind_gust",
    "cloudcover",
    "humidity",
    "visibility",
    "wind_speed",
]

to_drop = [
    "sunrise",
    "sunset",
    "moonrise",
    "moonset"
]

df = df.drop(*to_drop)





# UDF for converting column type from vector to double type
unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())

# Iterating over columns to be scaled
for i in tqdm(minmax_columns):
    # VectorAssembler Transformation - Converting column to vector type
    assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
    # MinMaxScaler Transformation
    scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
    # Pipeline of VectorAssembler and MinMaxScaler
    pipeline = Pipeline(stages=[assembler, scaler])

    # Fitting pipeline on dataframe
    df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")

unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())





# Iterating over columns to be scaled
for i in tqdm(sc_columns):
    # VectorAssembler Transformation - Converting column to vector type
    assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
    scaler = StandardScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
    # Pipeline of VectorAssembler and MinMaxScaler
    pipeline = Pipeline(stages=[assembler, scaler])

    # Fitting pipeline on dataframe
    df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")


df = df.drop(*sc_columns)
df = df.drop(*minmax_columns)





# Convert wind_dir to radians and calculate sine and cosine values
df = df.withColumn("wind_dir_rad", radians(df["wind_dir"]))
df = df.withColumn("wind_dir_sin", sin(df["wind_dir_rad"]))
df = df.withColumn("wind_dir_cos", cos(df["wind_dir_rad"]))




# Drop the original wind_dir column
df_scaled = df.drop("wind_dir")
df_scaled = df_scaled.drop("wind_dir_rad")




# Extract date components
df_scaled = df_scaled.withColumn("year", year("date_time"))
df_scaled = df_scaled.withColumn("month", month("date_time"))
df_scaled = df_scaled.withColumn("day", dayofmonth("date_time"))
df_scaled = df_scaled.withColumn("hour", hour("date_time"))
df_scaled = df_scaled.withColumn("minute", minute("date_time"))



# Calculate sine and cosine values for date components
df_scaled = df_scaled.withColumn("date_time_year_sin", sin(2 * 3.141592653589793238 * (year(df["date_time"]) - 2000) / 100))
df_scaled = df_scaled.withColumn("date_time_year_cos", cos(2 * 3.141592653589793238 * (year(df["date_time"]) - 2000) / 100))

df_scaled = df_scaled.withColumn("date_time_month_sin", sin(2 * 3.141592653589793238 * month(df["date_time"]) / 12))
df_scaled = df_scaled.withColumn("date_time_month_cos", cos(2 * 3.141592653589793238 * month(df["date_time"]) / 12))

df_scaled = df_scaled.withColumn("date_time_day_sin", sin(2 * 3.141592653589793238 * dayofmonth(df["date_time"]) / 31))
df_scaled = df_scaled.withColumn("date_time_day_cos", cos(2 * 3.141592653589793238 * dayofmonth(df["date_time"]) / 31))

df_scaled = df_scaled.withColumn("date_time_hour_sin", sin(2 * 3.141592653589793238 * hour(df["date_time"]) / 24))
df_scaled = df_scaled.withColumn("date_time_hour_cos", cos(2 * 3.141592653589793238 * hour(df["date_time"]) / 24))

df_scaled = df_scaled.withColumn("date_time_minute_sin", sin(2 * 3.141592653589793238 * minute(df["date_time"]) / 60))
df_scaled = df_scaled.withColumn("date_time_minute_cos", cos(2 * 3.141592653589793238 * minute(df["date_time"]) / 60))

# Drop original date_time and date components columns
df_scaled = df_scaled.drop("date_time", "year", "month", "day", "hour", "minute")




df.dropna()






# One-hot encode categorical variable 'city_id'
# encoder = OneHotEncoder(inputCols=["city_id"], outputCols=["city_id_encoded"])
# df_encoded = encoder.fit(df_scaled).transform(df_scaled)

categ = df_scaled.select('city_id').distinct().rdd.flatMap(lambda x:x).collect()
exprs = [F.when(F.col('city_id') == cat,1).otherwise(0)\
            .alias(str(cat)) for cat in categ]
df_encoded = df_scaled.select(exprs+df_scaled.columns)

# Drop original 'city_id' column
df_encoded = df_encoded.drop("city_id")





# Save the preprocessed data to a new table
table_name = "team29_projectdb.dataset_prepared_for_modeling"
df_encoded.write.mode("overwrite").saveAsTable(table_name)

print("Done data preparation")