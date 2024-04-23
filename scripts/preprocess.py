"""
    This module implements preprocessing
    of the initial dataset by combining
    all csv`s together, adding null values
    and creating a separate city file
    for city id mapping.
"""

import os
import glob
import pandas as pd

data_folder = os.path.join('data')
outdir = os.path.join(data_folder, 'preprocessed')
if not os.path.exists(outdir):
    os.mkdir(outdir)

# creating mapping of city to city_id

mapping = {}

csv_files = glob.glob(os.path.join(data_folder, '*.csv'))
for ind, file in enumerate(csv_files):
    name = file.split('.csv')[0].split('/')[1]
    mapping[name] = ind + 1


values = []
for city, city_id in mapping.items():
    values.append([city_id, city])
city = pd.DataFrame(values, columns=['city_id', 'city_name'])
city.to_csv(os.path.join(outdir, 'city.csv'),
            sep=',', header=True, index=False)

# combining all csv together


def update_csv(update_city_name, update_city_id):
    """
        This function preprocess a single csv of a city
        by renaming columns to sql-like type,
        adding corresponding city_id and null values
        of missing measurements.
    """
    city_df = pd.read_csv(os.path.join(data_folder, update_city_name + ".csv"))
    city_df.columns = [
        'date_time',
        'max_temp',
        'min_temp',
        'total_snow',
        'sun_hour',
        'uv_index_1',
        'uv_index_2',
        'moon_illumunation',
        'moonrise',
        'moonset',
        'sunrise',
        'sunset',
        'dew_point',
        'feels_like',
        'heat_index',
        'wind_chill',
        'wind_gust',
        'cloudcover',
        'humidity',
        'precip',
        'pressure',
        'temp',
        'visivility',
        'wind_dir',
        'wind_speed'
    ]

    def check(col_value):
        if 'no' in col_value.lower():
            return 'NULL'
        return col_value

    columns = ['moonset', 'moonrise']
    for col in columns:
        city_df[col] = city_df[col].apply(check)

    city_df['city_id'] = update_city_id
    return city_df


updated_csv = []
for city, city_id in mapping.items():
    updated_csv.append(update_csv(city, city_id))

combined = pd.concat(updated_csv, axis=0)
combined.to_csv(os.path.join(outdir, 'dataset.csv'),
                sep=',', header=True, index=False)

print('Done preprocessing')
