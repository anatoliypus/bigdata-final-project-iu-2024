#!/bin/bash

python3 scripts/build_projectdb.py

password=$(head -n 1 secrets/.psql.pass)
hdfs dfs -rm -r -f /user/team29/project/warehouse
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team29_projectdb --username team29 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=/user/team29/project/warehouse --m 1

rm -rf output/*.java
rm -rf output/*.avsc

mv *.java output/
mv *.avsc output/
