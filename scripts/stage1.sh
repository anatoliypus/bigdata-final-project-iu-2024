#!/bin/bash

python3 scripts/build_projectdb.py

rm -rf /home/team29/project/warehouse
mkdir -p /home/team29/project/warehouse

password=$(head -n 1 secrets/.psql.pass)
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team29_projectdb --username team29 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=/home/team29/project/warehouse --m 1

rm -rf output/*.java
rm -rf output/*.avsc

mv *.java output/
mv *.avsc output/
