#!/bin/bash

# Run data_preparation.py
spark-submit --master yarn scripts/data_preparation.py

# Run modeling.py
spark-submit --master yarn scripts/modeling.py