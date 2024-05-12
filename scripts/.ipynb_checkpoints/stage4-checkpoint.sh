#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

# Creaing Directory in Hadoop for storing the models scores
hdfs dfs -rm -r -f -skipTrash project/hive/warehouse/models_results
hdfs dfs -mkdir -p project/hive/warehouse/models_results/

# Putting the models_score file in Hadoop
hdfs dfs -put output/evaluation.csv project/hive/warehouse/models_results/

# Creating Hive Table with the models scores
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/models_scores.hql --hiveconf hive.resultset.use.unique.column.names=false