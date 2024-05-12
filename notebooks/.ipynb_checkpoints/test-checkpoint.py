from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, sin, cos, radians
from pyspark.ml.feature import StandardScaler, MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


team = 29

# location of your Hive database in HDFS
warehouse = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName("{} - spark ML".format(team))\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()


print("DONE----------------------------------------------------1")

# spark.sql("SHOW DATABASES").show()
# spark.sql("USE team29_projectdb").show()
# spark.sql("SHOW TABLES").show()
# spark.sql("SELECT * FROM team29_projectdb.dataset_part").show()


table_name = "team29_projectdb.dataset_part"
df = spark.read.table(table_name)

# Show DataFrame schema and preview data
# Get the number of rows
num_rows = df.count()

# Get the column names
columns = df.columns

# Get the number of columns
num_columns = len(columns)

print("Shape of DataFrame: ({}, {})".format(num_rows, num_columns))


print("DONE----------------------------------------------------2")

# Convert string columns to timestamp
df = df.withColumn("date_time", df["date_time"].cast("timestamp"))
df = df.withColumn("sunrise", df["sunrise"].cast("timestamp"))
df = df.withColumn("sunset", df["sunset"].cast("timestamp"))
df = df.withColumn("moonrise", df["moonrise"].cast("timestamp"))
df = df.withColumn("moonset", df["moonset"].cast("timestamp"))


df.printSchema()

df.show(35)

# # Define columns for scaling
# sc_columns = [
#     "max_temp",
#     "min_temp",
#     "feels_like",
#     "heat_index",
#     "wind_chill",
#     "precip",
#     "pressure",
#     "temp"
# ]

# minmax_columns = [
#     "sun_hour",
#     "uv_index_1",
#     "uv_index_2",
#     "moon_illumunation",
#     "dew_point",
#     "wind_gust",
#     "cloudcover",
#     "humidity",
#     "visibility",
#     "wind_speed",
# ]


# # UDF for converting column type from vector to double type
# unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())

# # Iterating over columns to be scaled
# for i in minmax_columns:
#     # VectorAssembler Transformation - Converting column to vector type
#     assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")

#     # MinMaxScaler Transformation
#     scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")

#     # Pipeline of VectorAssembler and MinMaxScaler
#     pipeline = Pipeline(stages=[assembler, scaler])

#     # Fitting pipeline on dataframe
#     df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")

# unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())

# # Iterating over columns to be scaled
# for i in sc_columns:
#     # VectorAssembler Transformation - Converting column to vector type
#     assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")

#     # MinMaxScaler Transformation
#     scaler = StandardScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")

#     # Pipeline of VectorAssembler and MinMaxScaler
#     pipeline = Pipeline(stages=[assembler, scaler])

#     # Fitting pipeline on dataframe
#     df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")


# print("DONE----------------------------------------------------3.0")

# df = df.drop(*sc_columns)
# df = df.drop(*minmax_columns)




# # print("DONE----------------------------------------------------3.5")

# # Convert wind_dir to radians and calculate sine and cosine values
# df = df.withColumn("wind_dir_rad", radians(df["wind_dir"]))
# df = df.withColumn("wind_dir_sin", sin(df["wind_dir_rad"]))
# df = df.withColumn("wind_dir_cos", cos(df["wind_dir_rad"]))


# # print("DONE----------------------------------------------------4")

# # # Drop the original wind_dir column
# df_scaled = df.drop("wind_dir")
# df_scaled = df_scaled.drop("wind_dir_rad")

# # Extract date components
# df_scaled = df_scaled.withColumn("year", year("date_time"))
# df_scaled = df_scaled.withColumn("month", month("date_time"))
# df_scaled = df_scaled.withColumn("day", dayofmonth("date_time"))
# df_scaled = df_scaled.withColumn("hour", hour("date_time"))
# df_scaled = df_scaled.withColumn("minute", minute("date_time"))

# # print("DONE----------------------------------------------------5")


# #from pyspark.sql.functions import col

# # Calculate sine and cosine values for date components
# df_scaled = df_scaled.withColumn("date_time_year_sin", sin(2 * 3.141592653589793238 * (year(df["date_time"]) - 2000) / 100))
# df_scaled = df_scaled.withColumn("date_time_year_cos", cos(2 * 3.141592653589793238 * (year(df["date_time"]) - 2000) / 100))

# df_scaled = df_scaled.withColumn("date_time_month_sin", sin(2 * 3.141592653589793238 * month(df["date_time"]) / 12))
# df_scaled = df_scaled.withColumn("date_time_month_cos", cos(2 * 3.141592653589793238 * month(df["date_time"]) / 12))

# df_scaled = df_scaled.withColumn("date_time_day_sin", sin(2 * 3.141592653589793238 * dayofmonth(df["date_time"]) / 31))
# df_scaled = df_scaled.withColumn("date_time_day_cos", cos(2 * 3.141592653589793238 * dayofmonth(df["date_time"]) / 31))

# df_scaled = df_scaled.withColumn("date_time_hour_sin", sin(2 * 3.141592653589793238 * hour(df["date_time"]) / 24))
# df_scaled = df_scaled.withColumn("date_time_hour_cos", cos(2 * 3.141592653589793238 * hour(df["date_time"]) / 24))

# df_scaled = df_scaled.withColumn("date_time_minute_sin", sin(2 * 3.141592653589793238 * minute(df["date_time"]) / 60))
# df_scaled = df_scaled.withColumn("date_time_minute_cos", cos(2 * 3.141592653589793238 * minute(df["date_time"]) / 60))

# # Drop original date_time and date components columns
# df_scaled = df_scaled.drop("date_time", "year", "month", "day", "hour", "minute")

# print("DONE----------------------------------------------------6")

               
                                 
                                 
# def convert_dumb_dates(df, column_names):
#     for column_name in column_names:
#         # Extract hour and minute from timestamp
#         df = df.withColumn(f'{column_name}_hour', hour(df[column_name]))
#         df = df.withColumn(f'{column_name}_minute', minute(df[column_name]))
        
#         # Calculate sine and cosine for hour
#         df = df.withColumn(f'{column_name}_hour_sin', sin(2 * 3.141592653589793238 * df[column_name+"_hour"] / 24))
#         df = df.withColumn(f'{column_name}_hour_cos', cos(2 * 3.141592653589793238 * df[column_name+"_hour"] / 24))

#         # Calculate sine and cosine for minute
#         df = df.withColumn(f'{column_name}_minute_sin',
#                            sin(2 * 3.141592653589793238 * df[f"{column_name}_minute"] / 60))
        
#         df = df.withColumn(f'{column_name}_minute_cos',
#                            cos(2 * 3.141592653589793238 * df[f"{column_name}_minute"] / 60))

#         # Drop original timestamp, hour, and minute columns
#         df = df.drop(column_name, f'{column_name}_hour', f'{column_name}_minute')

#     return df


# # Apply convert_dumb_dates function to DataFrame
# df_scaled = convert_dumb_dates(df_scaled, ['moonrise', 'moonset', 'sunrise', 'sunset'])

# print("DONE----------------------------------------------------7")


# df_scaled.show()
# print(df_scaled.columns)
# print((df_scaled.count(), len(df_scaled.columns)))


# # Drop rows with missing values
# df_scaled.fillna(value=0)

# # One-hot encode categorical variable 'city_id'
# encoder = OneHotEncoder(inputCols=["city_id"], outputCols=["city_id_encoded"])
# df_encoded = encoder.fit(df_scaled).transform(df_scaled)

# print("DONE----------------------------------------------------8")
# # Drop original 'city_id' column
# df_encoded = df_encoded.drop("city_id")

# df_encoded.show()
# print(df_encoded.columns)
# print((df_encoded.count(), len(df_encoded.columns)))


# # # Write preprocessed data to CSV file
# # # output_path = "hdfs:///user/hive/warehouse/preprocessed.csv"  # Adjust the path as needed
# # # df_encoded.write.mode("overwrite").csv(output_path, header=True)

