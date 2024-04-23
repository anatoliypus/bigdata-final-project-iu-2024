import pandas as pd
import os
import glob

data_folder = os.path.join('data')
outdir = os.path.join(data_folder, 'preprocessed')
if not os.path.exists(outdir):
    os.mkdir(outdir)

# creating mapping of city to city_id

mapping = dict()

csv_files = glob.glob(os.path.join(data_folder, '*.csv'))
for ind, file in enumerate(csv_files):
    name = file.split('.csv')[0].split('/')[1]
    mapping[name] = ind + 1


values = []
for city, city_id in mapping.items():
    values.append([city_id, city])
city = pd.DataFrame(values, columns=['city_id', 'city_name'])
city.to_csv(os.path.join(outdir, 'city.csv'), sep=',', header=True, index=False)

# combining all csv together

def update_csv(city_name, city_id):
    df = pd.read_csv(os.path.join(data_folder, city_name + ".csv"))
    df.columns = [
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

    def check(v):
        if ('no' in v.lower()):
            return 'NULL'
        return v
    
    columns = ['moonset', 'moonrise']
    for col in columns:
        df[col] = df[col].apply(check)

    df['city_id'] = city_id
    return df

updated_csv = [] 
for city, city_id in mapping.items():
    updated_csv.append(update_csv(city, city_id))

combined = pd.concat(updated_csv, axis=0)
combined.to_csv(os.path.join(outdir, 'dataset.csv'), sep=',', header=True, index=False)

print('Done preprocessing')
