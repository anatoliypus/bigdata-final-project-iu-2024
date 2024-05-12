"""
Module for preprocessing initial datasets by combining CSV files, adding null values,
and creating a separate city file for city ID mapping.
"""

import os
import glob
import pandas as pd

DATA_FOLDER = os.path.join('data')
OUTDIR = os.path.join(DATA_FOLDER, 'preprocessed')

if not os.path.exists(OUTDIR):
    os.makedirs(OUTDIR)

def create_city_mapping():
    """
    Create and return a dictionary mapping city names to city IDs based on CSV filenames.
    """
    mapping = {}
    csv_files = glob.glob(os.path.join(DATA_FOLDER, '*.csv'))
    for ind, file in enumerate(csv_files):
        name = os.path.basename(file).split('.csv')[0]
        mapping[name] = ind + 1
    return mapping

def save_city_mapping(mapping):
    """
    Save the city to city ID mapping to a CSV file.
    """
    values = [[city_id, city] for city, city_id in mapping.items()]
    city_df = pd.DataFrame(values, columns=['city_id', 'city_name'])
    city_df.to_csv(os.path.join(OUTDIR, 'city.csv'), sep=',', header=True, index=False)

def preprocess_city_data(city_name, city_id):
    """
    Preprocess a single city CSV file by renaming columns, adding a city_id, and handling null values.
    """
    file_path = os.path.join(DATA_FOLDER, f"{city_name}.csv")
    city_df = pd.read_csv(file_path)
    city_df.columns = [
        'date_time', 'max_temp', 'min_temp', 'total_snow', 'sun_hour', 'uv_index_1', 'uv_index_2',
        'moon_illumination', 'moonrise', 'moonset', 'sunrise', 'sunset', 'dew_point', 'feels_like',
        'heat_index', 'wind_chill', 'wind_gust', 'cloudcover', 'humidity', 'precip', 'pressure',
        'temp', 'visibility', 'wind_dir', 'wind_speed'
    ]

    def replace_missing_values(value):
        """
        Replace specific missing values with 'NULL'.
        """
        return 'NULL' if 'no' in str(value).lower() else value

    for column in ['moonset', 'moonrise']:
        city_df[column] = city_df[column].apply(replace_missing_values)

    city_df['city_id'] = city_id
    return city_df

def combine_and_save_data(mapping):
    """
    Combine all city CSVs into a single dataset and save it.
    """
    updated_csvs = [preprocess_city_data(city, city_id) for city, city_id in mapping.items()]
    combined_df = pd.concat(updated_csvs, axis=0)
    combined_df.to_csv(os.path.join(OUTDIR, 'dataset.csv'), sep=',', header=True, index=False)

def main():
    """
    Main function to control the preprocessing workflow.
    """
    city_mapping = create_city_mapping()
    save_city_mapping(city_mapping)
    combine_and_save_data(city_mapping)
    print('Done preprocessing')

if __name__ == "__main__":
    main()