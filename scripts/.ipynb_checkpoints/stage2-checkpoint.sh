#!/bin/bash
hdfs dfs -mkdir -p /user/team29/project/warehouse/avsc
hdfs dfs -put output/*.avsc /user/team29/project/warehouse/avsc

# Creating the Hive Database
password=$(head -n 1 secrets/.hive.pass)
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/db.hql > output/hive_results.txt

# Running Queries of EDA
hdfs dfs -rm -r -f -skipTrash project/hive/warehouse/q1
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q1.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q1.csv

hdfs dfs -rm -r -f -skipTrash project/hive/warehouse/q2
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q2.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q2.csv

hdfs dfs -rm -r -f -skipTrash project/hive/warehouse/q3
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q3.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q3.csv

hdfs dfs -rm -r -f -skipTrash project/hive/warehouse/q4
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q4.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q4.csv

hdfs dfs -rm -r -f -skipTrash project/hive/warehouse/q5
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q5.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q5.csv
