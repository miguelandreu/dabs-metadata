# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS meteo_open_data.operations.metadata

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS meteo_open_data.operations.metadata (
    operation_name  STRING,
    ingestion_name  STRING,
    operation_type  STRING,
    parameters      STRING,
    is_active       BOOLEAN,
    creation_date   TIMESTAMP
)
COMMENT 'parameters field stores JSON-serialized configuration per operation type'
""")


# COMMAND ----------

import json

parameters = json.dumps({
    "base_volume_path": "/Volumes/meteo_open_data/landing/raw_api_landing",
    "base_url": "https://datos.tenerife.es/api/meteo/latest",
    "endpoint": "/measures"
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'api_measures',
    'api',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------

import json

parameters = json.dumps({
    "base_volume_path": "/Volumes/meteo_open_data/landing/raw_api_landing",
    "base_url": "https://datos.tenerife.es/api/meteo/latest",
    "endpoint": "/stations"
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'api_stations',
    'api',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------



import json

parameters = json.dumps({
    "base_volume_path": "/Volumes/meteo_open_data/landing/raw_api_landing",
    "base_url": "https://datos.tenerife.es/api/meteo/latest",
    "endpoint": "/stations/{idWeatherstation}/sensors",
    "dependencies": [{
        "endpoint": "/stations",
        "key": "id_weatherstation"}]
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'api_sensors',
    'api',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------



import json

parameters = json.dumps({
    "base_volume_path": "/Volumes/meteo_open_data/landing/raw_api_landing",
    "base_url": "https://datos.tenerife.es/api/meteo/latest",
    "endpoint": "/readings/summarized/station/{idWeatherstation}/year/{year}",
    "dependencies": [{
        "endpoint": "/stations",
        "key": "id_weatherstation"}]
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'bronze_reading_summarized',
    'api',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------

import json

schema = {
    "type": "struct",
    "fields": [
            {"name": "id_weatherdatatype", "type": "integer", "nullable": False},
            {"name": "alias",              "type": "string",  "nullable": True},
            {"name": "name",               "type": "string",  "nullable": True},
            {"name": "unit",               "type": "string",  "nullable": True},
        ]
}

parameters = json.dumps({
    "source_path": "/Volumes/meteo_open_data/landing/raw_api_landing/measures",
    "file_format": "json",
    "target_table": "meteo_open_data.bronze.measures_tenerife",
    "schema": schema
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'bronze_measures',
    'bronze',
    '{parameters}',
    True,
    current_timestamp()
)
""")

# COMMAND ----------


import json

schema = {
    "type": "struct",
    "fields": [
        {"name": "id_weatherstation", "type": "integer", "nullable": False},
        {"name": "name",              "type": "string",  "nullable": True},
        {"name": "date_install",      "type": "timestamp","nullable": True},
        {"name": "date_uninstall",    "type": "string",  "nullable": True},
        {"name": "municipality_id",   "type": "integer", "nullable": True},
        {"name": "municipality_name", "type": "string",  "nullable": True},
        {"name": "locality",          "type": "string",  "nullable": True},
        {"name": "place",             "type": "string",  "nullable": True},
        {"name": "latitude",          "type": "decimal", "nullable": True},
        {"name": "longitude",         "type": "decimal", "nullable": True},
        {"name": "x_cords",           "type": "integer", "nullable": True},
        {"name": "y_cords",           "type": "integer", "nullable": True},
        {"name": "altitude",          "type": "integer", "nullable": True},
        {"name": "datalogger_type",   "type": "string",  "nullable": True},
        {"name": "sensors_count",     "type": "integer", "nullable": True},
    ]
}

parameters = json.dumps({
    "source_path": "/Volumes/meteo_open_data/landing/raw_api_landing/stations",
    "file_format": "json",
    "target_table": "meteo_open_data.bronze.stations_tenerife",
    "schema": schema
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'bronze_stations',
    'bronze',
    '{parameters}',
    True,
    current_timestamp()
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM meteo_open_data.operations.metadata WHERE ingestion_name = "bronze_readings"

# COMMAND ----------

import json

schema = {
    "type": "struct",
    "fields": [
        {"name": "id_weatherstationsensor", "type": "integer", "nullable": False},
        {"name": "sensor_alias",            "type": "string",  "nullable": True},
        {"name": "sensor_name",             "type": "string",  "nullable": True},
        {"name": "unit",                    "type": "string",  "nullable": True},
        {"name": "model",                   "type": "string",  "nullable": True},
        {"name": "manufacturer",            "type": "string",  "nullable": True},
        {"name": "installation_type",       "type": "string",  "nullable": True},
    ]
}

parameters = json.dumps({
    "source_path": "/Volumes/meteo_open_data/landing/raw_api_landing/sensors",
    "file_format": "json",
    "target_table": "meteo_open_data.bronze.sensors_tenerife",
    "schema": schema
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'bronze_sensors',
    'bronze',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------

import json

schema = {
    "type": "struct",
    "fields": [
        {"name": "id_weatherstation", "type": "integer", "nullable": False},
        {
            "name": "sensors",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {"name": "id_weatherstationsensor", "type": "integer", "nullable": False},
                        {
                            "name": "dates",
                            "type": {
                                "type": "array",
                                "elementType": {
                                    "type": "struct",
                                    "fields": [
                                        {"name": "observation_date", "type": "date",    "nullable": True},
                                        {"name": "is_validated",     "type": "boolean", "nullable": True},
                                        {
                                            "name": "values",
                                            "type": {
                                                "type": "array",
                                                "elementType": {
                                                    "type": "struct",
                                                    "fields": [
                                                        {"name": "mean",  "type": "double", "nullable": True},
                                                        {"name": "min",   "type": "double", "nullable": True},
                                                        {"name": "max",   "type": "double", "nullable": True},
                                                        {"name": "total", "type": "double", "nullable": True},
                                                    ]
                                                },
                                                "containsNull": True
                                            },
                                            "nullable": True
                                        }
                                    ]
                                },
                                "containsNull": True
                            },
                            "nullable": True
                        }
                    ]
                },
                "containsNull": True
            },
            "nullable": True
        }
    ]
}

parameters = json.dumps({
    "source_path": "/Volumes/meteo_open_data/landing/raw_api_landing/readings",
    "file_format": "json",
    "target_table": "meteo_open_data.bronze.readings_tenerife",
    "schema": schema
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'bronze_readings',
    'bronze',
    '{parameters}',
    True,
    current_timestamp()
)
""")

# COMMAND ----------

import json

parameters = json.dumps({
    "source_table": "meteo_open_data.bronze.stations_tenerife",
    "target_table": "meteo_open_data.silver.stations_tenerife_cdc",
    "cdc_parameters": 
        {"cdc_type":"auto_cdc_from_snapshot", "keys": ["id_weatherstation"], "stored_as_scd_type": "2", "track_history_except_column_list": ["source_metadata", "file_date"], "order_keys": ["_ingestion_timestamp", "file_date"]}
    
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'stations_tenerife_cdc',
    'silver',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------

import json

parameters = json.dumps({
    "source_table": "meteo_open_data.bronze.measures_tenerife",
    "target_table": "meteo_open_data.silver.measures_tenerife_cdc",
    "cdc_parameters": 
        {"cdc_type":"auto_cdc_from_snapshot", "keys": ["id_weatherdatatype"], "stored_as_scd_type": "2", "track_history_except_column_list": ["source_metadata", "file_date"], "order_keys": ["_ingestion_timestamp", "file_date"]}
    
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'measures_tenerife_cdc',
    'silver',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------

import json

parameters = json.dumps({
    "source_table": "meteo_open_data.bronze.sensors_tenerife",
    "target_table": "meteo_open_data.silver.sensors_tenerife_cdc",
    "cdc_parameters": 
        {"cdc_type":"auto_cdc_from_snapshot", "keys": ["id_weatherstationsensor"], "stored_as_scd_type": "2", "track_history_except_column_list": ["source_metadata", "file_date"], "order_keys": ["_ingestion_timestamp", "file_date"]}
    
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'sensors_tenerife_cdc',
    'silver',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------

import json

parameters = json.dumps({
    "source_table": "meteo_open_data.bronze.readings_tenerife",
    "target_table": "meteo_open_data.silver.readings_tenerife_cdc",
    "cdc_parameters": 
        {"cdc_type":"auto_cdc_from_snapshot", "keys": ["id_weatherstation"], "stored_as_scd_type": "2", "track_history_except_column_list": ["source_metadata", "file_date"], "order_keys": ["_ingestion_timestamp", "file_date"]}
    
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'readings_tenerife_cdc',
    'silver',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------

import json

parameters = json.dumps({
    "sql_file_path": "/Volumes/meteo_open_data/operations/metadata_storage/gold_layer/tenerife_meteo_open_gold_layer.sql"
    
})

spark.sql(f"""
INSERT INTO meteo_open_data.operations.metadata
(operation_name, ingestion_name, operation_type, parameters, is_active, creation_date)
VALUES (
    'tenerife_meteo_data',
    'gold_layer',
    'gold',
    '{parameters}',
    true,
    current_timestamp()
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM meteo_open_data.operations.metadata
# MAGIC WHERE operation_type = 'bronze'