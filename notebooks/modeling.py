from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, sin, cos, radians
from pyspark.ml.feature import StandardScaler, MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from tqdm import tqdm
import pandas as pd







class Multitarget_Model:
    def __init__(self, feature_columns, target_columns, model_type):
        self.feature_columns = feature_columns
        self.target_columns = target_columns
        self.models = []
        self.best_model = None
        self.model_type = model_type


    def fit(self, df, cv=False, cv_folds=3):
        for target in tqdm(self.target_columns, desc="Training...", unit="model"):
            # Define the assembler
            assembler = VectorAssembler(inputCols=self.feature_columns, outputCol="features")

            # Define the primary model
            if self.model_type == "GBT":
                primary_model = GBTRegressor(featuresCol="features", labelCol=target, maxIter=10)
            elif self.model_type == "RF":
                primary_model = RandomForestRegressor(featuresCol="features", labelCol=target)
            else:
                raise ValueError("Model type not supported")

            # Define the pipeline
            pipeline = Pipeline(stages=[assembler, primary_model])


            if cv:
                # Define the evaluator
                evaluator = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="rmse")

                # Define the parameter grid
                if self.model_type == "GBT":
                    paramGrid = ParamGridBuilder() \
                        .addGrid(primary_model.maxDepth, [1, 3]) \
                        .addGrid(primary_model.maxBins, [5, 10]) \
                        .build()
                elif self.model_type == "RF":
                    paramGrid = ParamGridBuilder() \
                        .addGrid(primary_model.maxDepth, [1, 3]) \
                        .addGrid(primary_model.numTrees, [5, 10]) \
                        .build()
                else:
                    raise ValueError("Model type not supported")

                # Define the cross-validator
                crossval = CrossValidator(estimator=pipeline,
                                        estimatorParamMaps=paramGrid,
                                        evaluator=evaluator,
                                        numFolds=cv_folds)

                # Fit the model
                model = crossval.fit(df)

            else:
                # Fit the model
                model = pipeline.fit(df)


            # Append the model to the list of models
            self.models.append(model)


    def evaluate(self, df, metric):

        # Create a dictionary to store the results and fill it with zeros
        results = {target: 0 for target in self.target_columns}

        for i, model in tqdm(enumerate(self.models), desc="Evaluating...", unit="model"):
            # Get the target column
            target_col = self.target_columns[i]

            # Get the predictions
            predictions = model.transform(df)

            # Get the evaluator
            evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName=metric)

            # Evaluate the model
            result = evaluator.evaluate(predictions)

            # Store the result
            results[target_col] = result

        return results


    def predict(self, df):
        # Create an empty dataframe to store the predictions
        predictions = None

        for i, model in tqdm(enumerate(self.models), desc="Predicting... ", unit="model"):

            # Get the predictions
            prediction = model.transform(df)

            # Select only the prediction column
            prediction = prediction.select("ID", "prediction")

            # Rename the prediction column to the target column
            name = self.target_columns[i].replace("target_", "prediction_")
            prediction = prediction.withColumnRenamed("prediction", name)

            # If the predictions dataframe is empty, assign the prediction dataframe to it
            if predictions is None:
                predictions = prediction

            # Otherwise, join the prediction dataframe with the predictions dataframe
            else:
                predictions = predictions.join(prediction, "ID", "inner")

        return predictions


    def save(self):
        # Save the models in location like project/models/model1/
        for i, model in enumerate(self.models):
            model.write().overwrite().save(f"project/models/{self.model_type}/model_{i}")

            




# --------------------STARTING SPARK SESSION--------------------
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


table_name = "team29_projectdb.dataset_prepared_for_modeling"
df = spark.read.table(table_name)



# --------------------BASIC DATA PROCESSING AND SPLIT--------------------
CITY_TO_PREDICT = 1


# Get only rows with column "1" is equal to 1
city_df = df.filter(col(f"{CITY_TO_PREDICT}") == CITY_TO_PREDICT)

# Drop the city encoded columns
city_df = city_df.drop("1", "2", "3", "4", "5", "6", "7", "8")

# Create an ID column
city_df = city_df.withColumn("ID", F.monotonically_increasing_id())





# Target dataframe is all except first row
city_target = city_df.filter(city_df.ID != 0)
# Update ID to start from zero (id - 1)
city_target = city_target.withColumn("ID", city_target["ID"] - 1)
# Add "target" to all column names except "ID"
city_target = city_target.toDF(*["target_" + c if c != "ID" else c for c in city_target.columns])

# feature dataframe is all except last row
city_feature = city_df.filter(city_df.ID != city_df.count() - 1)
# Add "feature" to all column names except "ID"
city_feature = city_feature.toDF(*["feature_" + c if c != "ID" else c for c in city_feature.columns])




# Merge into a single dataframe on "ID"
city_merged = city_target.join(city_feature, "ID", "inner")




# Define the feature and target columns
feature_columns = [c for c in city_merged.columns if c.startswith("feature_")]
target_columns = [c for c in city_merged.columns if c.startswith("target_")]

# split the data
train, test = city_merged.randomSplit([0.8, 0.2])





# --------------------TRAINING AND EVALUATION OF GBT WITHOUT OPTIMIZATION--------------------
# Initialize the model
model = Multitarget_Model(feature_columns, target_columns, model_type="GBT")

# Fit the model
model.fit(train, cv=False)




# Evaluate the model
results = model.evaluate(test, "rmse")

# Print the results
for target, result in results.items():
    print("RMSE for {}: {}".format(target, result))

# Evaluate the model
results = model.evaluate(test, "r2")

# Print the results
for target, result in results.items():
    print("R2 for {}: {}".format(target, result))





# --------------------TRAINING, EVALUATION, AND PREDICTION OF GBT WITH OPTIMIZATION--------------------
# Initialize the model
model = Multitarget_Model(feature_columns, target_columns, model_type="GBT")

# Fit the model
model.fit(train, cv=True, cv_folds=2)




# Evaluate the model
rmse_GBT = model.evaluate(test, "rmse")

# Print the results
for target, result in rmse_GBT.items():
    print("RMSE for {}: {}".format(target, result))

# Evaluate the model
r2_GBT = model.evaluate(test, "r2")

# Print the results
for target, result in r2_GBT.items():
    print("R2 for {}: {}".format(target, result))




# Save the best model
model.save()




# Predict the test data and save it into csv
predictions = model.predict(test)

# merge predictions with test data
results = test.join(predictions, "ID", "inner")

# Drop everything except the target and prediction columns
results = results.select([c for c in results.columns if c.startswith("target_") or c.startswith("prediction_")])

# Save into project/output/GBT_predictions on HDFS (as a one partition)
results.coalesce(1).write.mode("overwrite").csv("project/output/GBT_predictions")






# --------------------TRAINING AND EVALUATION OF RF WITHOUT OPTIMIZATION--------------------
# Initialize the model
model = Multitarget_Model(feature_columns, target_columns, model_type="RF")

# Fit the model
model.fit(train, cv=False)




# Evaluate the model
results = model.evaluate(test, "rmse")

# Print the results
for target, result in results.items():
    print("RMSE for {}: {}".format(target, result))

# Evaluate the model
results = model.evaluate(test, "r2")

# Print the results
for target, result in results.items():
    print("R2 for {}: {}".format(target, result))





# --------------------TRAINING, EVALUATION, AND PREDICTION OF RF WITH OPTIMIZATION--------------------
# Initialize the model
model = Multitarget_Model(feature_columns, target_columns, model_type="RF")

# Fit the model
model.fit(train, cv=True, cv_folds=2)




# Evaluate the model
rmse_RF = model.evaluate(test, "rmse")

# Print the results
for target, result in rmse_RF.items():
    print("RMSE for {}: {}".format(target, result))

# Evaluate the model
r2_RF = model.evaluate(test, "r2")

# Print the results
for target, result in r2_RF.items():
    print("R2 for {}: {}".format(target, result))




# Save the best model
model.save()



# Predict the test data and save it into csv
predictions = model.predict(test)

# merge predictions with test data
results = test.join(predictions, "ID", "inner")

# Drop everything except the target and prediction columns
results = results.select([c for c in results.columns if c.startswith("target_") or c.startswith("prediction_")])

# Save into project/output/GBT_predictions on HDFS (as a one partition)
results.coalesce(1).write.mode("overwrite").csv("project/output/RF_predictions")




# --------------------GENERATION OF COMPARISON TABLE--------------------
# The format of table is: target | RMSE (GBT) | R2 (GBT) | RMSE (RF) | R2 (RF)
table = pd.DataFrame(columns=["target", "RMSE (GBT)", "R2 (GBT)", "RMSE (RF)", "R2 (RF)"])

for target in rmse_GBT.keys():
    table = table.append({
        "target": target.replace("target_", ""),
        "RMSE (GBT)": rmse_GBT[target],
        "R2 (GBT)": r2_GBT[target],
        "RMSE (RF)": rmse_RF[target],
        "R2 (RF)": r2_RF[target]
    }, ignore_index=True)

# save in HDFS as a csv file in project/output/evaluation
table.to_csv("project/output/results_comparison.csv", index=False)