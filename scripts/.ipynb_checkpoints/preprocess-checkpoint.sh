#!/bin/bash

rm -rf data/*.csv
rm -rf data/preprocessed

url="https://disk.yandex.ru/d/VgQS1Kcjrs6O9Q"
wget "$(yadisk-direct $url)" -O data/data.zip

unzip data/data.zip -d data/
rm data/data.zip

python3 scripts/preprocess.py
rm -rf data/*.csv
mv data/preprocessed/*.csv data/
rm -rf data/preprocessed
