-- Optional
-- but it is useful if you want to not commit the change when some errors happened before the commit statement
START TRANSACTION;

-- Drop existing tables

DROP TABLE IF EXISTS dataset;
DROP TABLE IF EXISTS city;


-- Add tables
-- city table
CREATE TABLE IF NOT EXISTS city (
    city_id integer NOT NULL PRIMARY KEY,
    city_name VARCHAR(20)
);

-- dataset table
CREATE TABLE IF NOT EXISTS dataset (
    date_time timestamp NOT NULL,
    max_temp INTEGER NOT NULL,
    min_temp INTEGER NOT NULL,
    total_snow NUMERIC(10, 5) NOT NULL,
    sun_hour NUMERIC(10, 5) NOT NULL,
    uv_index_1 INTEGER NOT NULL,
    uv_index_2 INTEGER NOT NULL,
    moon_illumunation INTEGER NOT NULL,
    moonrise TIME,
    moonset TIME,
    sunrise TIME NOT NULL,
    sunset TIME NOT NULL,
    dew_point INTEGER NOT NULL,
    feels_like INTEGER NOT NULL,
    heat_index INTEGER NOT NULL,
    wind_chill INTEGER NOT NULL,
    wind_gust INTEGER NOT NULL,
    cloudcover INTEGER NOT NULL,
    humidity INTEGER NOT NULL,
    precip NUMERIC(10, 5) NOT NULL,
    pressure INTEGER NOT NULL,
    temp INTEGER NOT NULL,
    visivility INTEGER NOT NULL,
    wind_dir INTEGER NOT NULL,
    wind_speed INTEGER NOT NULL,
    city_id integer NOT NULL
);

-- Add constraints
-- FKs
ALTER TABLE dataset DROP CONSTRAINT IF EXISTS fk_dataset_cityid_cityid;
ALTER TABLE dataset ADD CONSTRAINT fk_dataset_cityid_cityid FOREIGN KEY(city_id) REFERENCES city (city_id);

COMMIT;
