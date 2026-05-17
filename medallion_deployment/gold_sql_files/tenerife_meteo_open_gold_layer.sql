CREATE OR REFRESH MATERIALIZED VIEW meteo_open_data.gold.readings_tenerife as (
with transform_1 as (
SELECT
    id_weatherstation,
    explode(sensors) as sensors
FROM meteo_open_data.silver.readings_tenerife_cdc
WHERE `__END_AT` IS NULL
),
transform_2 as (
SELECT
    id_weatherstation,
    sensors.id_weatherstationsensor,
    explode(sensors.dates) record_date
FROM transform_1
),
transform_3 as (
SELECT
    id_weatherstation,
    id_weatherstationsensor,
    record_date.observation_date,
    explode(record_date.values) date_values
FROM transform_2
WHERE record_date.is_validated = 'true'
)
SELECT
    id_weatherstation,
    id_weatherstationsensor,
    observation_date,
    date_values.mean,
    date_values.min,
    date_values.max,
    date_values.total
FROM transform_3 tr_3
);

CREATE OR REFRESH MATERIALIZED VIEW meteo_open_data.gold.stations_tenerife as (
    SELECT
    id_weatherstation,
    name,
    date_install,
    date_uninstall,
    municipality_id,
    municipality_name,
    locality,
    place,
    latitude,
    longitude,
    sensors_count
FROM meteo_open_data.silver.stations_tenerife_cdc
WHERE `__END_AT` IS NULL
);

CREATE OR REFRESH MATERIALIZED VIEW meteo_open_data.gold.measures_tenerife as (
SELECT
    id_weatherdatatype,
    alias,
    name,
    unit
FROM meteo_open_data.silver.measures_tenerife_cdc
WHERE `__END_AT` IS NULL
);

CREATE OR REFRESH MATERIALIZED VIEW meteo_open_data.gold.sensors_tenerife as (
SELECT
    id_weatherstationsensor,
    sensor_alias,
    sensor_name,
    unit,
    model,
    manufacturer,
    installation_type
FROM meteo_open_data.silver.sensors_tenerife_cdc
WHERE __END_AT IS NULL
);