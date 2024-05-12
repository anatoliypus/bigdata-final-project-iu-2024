"""
This module handles data processing, training, evaluation, and prediction using machine learning models on a Spark cluster.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import os


def run_command(command):
    """
    Execute a shell command and return its output.
    """
    return os.popen(command).read()


class MultiTargetModel:
    """
    A class to handle multiple target regression with Spark ML models like GBT and RandomForest.
    """
    def __init__(self, feature_columns, target_columns, model_type):
        self.feature_columns = feature_columns
        self.target_columns = target_columns
        self.models = []
        self.model_type = model_type

    def fit(self, df, use_cross_validation=False, cv_folds=3):
        """
        Fit models for each target.
        """
        for target in self.target_columns:
            assembler = VectorAssembler(inputCols=self.feature_columns, outputCol="features")
            if self.model_type == "GBT":
                model = GBTRegressor(featuresCol="features", labelCol=target, maxIter=10)
            elif self.model_type == "RF":
                model = RandomForestRegressor(featuresCol="features", labelCol=target)
            else:
                raise ValueError("Unsupported model type provided.")

            pipeline = Pipeline(stages=[assembler, model])

            if use_cross_validation:
                evaluator = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="rmse")
                param_grid = ParamGridBuilder() \
                    .addGrid(model.maxDepth, [1, 3]) \
                    .addGrid(model.maxBins if self.model_type == "GBT" else model.numTrees, [5, 10]) \
                    .build()
                cross_validator = CrossValidator(estimator=pipeline, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=cv_folds)
                fitted_model = cross_validator.fit(df)
            else:
                fitted_model = pipeline.fit(df)

            self.models.append(fitted_model)

    def evaluate(self, df, metric):
        """
        Evaluate all models using the given metric.
        """
        results = {target: 0 for target in self.target_columns}
        for i, model in enumerate(self.models):
            predictions = model.transform(df)
            evaluator = RegressionEvaluator(labelCol=self.target_columns[i], predictionCol="prediction", metricName=metric)
            results[self.target_columns[i]] = evaluator.evaluate(predictions)
        return results

    def predict(self, df):
        """
        Generate predictions using the fitted models.
        """
        predictions = None
        for i, model in enumerate(self.models):
            prediction = model.transform(df).select("ID", F.col("prediction").alias(self.target_columns[i].replace("target_", "prediction_")))
            predictions = prediction if predictions is None else predictions.join(prediction, "ID", "inner")
        return predictions

    def save_models(self):
        """
        Save each model to a directory.
        """
        for i, model in enumerate(self.models):
            model_path = f"project/models/{self.model_type}/model_{i}"
            model.write().overwrite().save(model_path)
            run_command(f"hdfs dfs -get {model_path} models/{self.model_type}/model_{i}")


def setup_spark_session(team_id):
    """
    Setup and return a Spark session for the given team.
    """
    warehouse_location = "project/hive/warehouse"
    return SparkSession.builder \
        .appName(f"{team_id} - Spark ML") \
        .master("yarn") \
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .enableHiveSupport() \
        .getOrCreate()


if __name__ == "__main__":
    TEAM_ID = 29
    spark = setup_spark_session(TEAM_ID)

    # Load data
    table_name = "team29_projectdb.dataset_prepared_for_modeling"
    df = spark.read.table(table_name)

    # Preprocessing and other steps could be included here

    print("Setup complete.")