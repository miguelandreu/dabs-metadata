# dlt_creator

A Databricks DLT pipeline creator package for building medallion-style Bronze, Silver, and Gold layers.

This package contains utilities for creating schema-driven ingest tables, automating CDC flows, and converting SQL materialized views into Python decorator-based DLT definitions.

## Overview

`dlt_creator` is designed to:

- read pipeline metadata from a Databricks table
- create Bronze tables using Auto Loader and structured schema definitions
- build Silver CDC flows for incremental and snapshot-based merge logic
- generate Gold materialized views from SQL files via Python decorator conversion

## Package layout

```text
dlt_creator/
  main.py
  pyproject.toml
  README.md
  dlt_creator/
    __init__.py
    transformation.py
    utils/
      __init__.py
      utils.py
```

## Requirements

- Python 3.11+
- `pyspark`
- `databricks-dlt` if running DLT flows outside of Databricks deployment context

Dependencies are declared in `dlt_creator/pyproject.toml`.

## Usage

### Running l

In Databricks, the main pipeline logic is driven from `dlt_creator/dlt_creator/transformation.py` by invoking `runner(spark)`.

Example notebook or job entrypoint:

```python
from dlt_creator.dlt_creator.transformation import runner

runner(spark)
```

### Metadata-driven pipeline execution

`runner(spark)` reads metadata from the table:

- `meteo_open_data.operations.metadata`

It filters active operations by `operation_name` and groups them by
`operation_type`:

- `bronze`
- `silver`
- `gold`

The `operation_name` is obtained from Spark configuration:

```python
spark.conf.get("operation_name", "tenerife_meteo_data")
```

## Bronze layer

For each bronze operation, the metadata `parameters` JSON is parsed and passed to `create_table(spark, parameters)`.

`create_table()` performs the following:

- reads streaming input using Auto Loader via `cloudFiles`
- applies schema defined in `parameters['schema']`
- adds `_metadata` and `_ingestion_timestamp`
- extracts `file_date` from the source file name

Required parameter keys include:

- `target_table`
- `source_path`
- `schema`
- `file_format`

## Silver layer

Silver operations support change-data-capture (CDC) flows.

The metadata `parameters` object must include `cdc_parameters`, and the method validates:

- `cdc_type`
- `source_table`
- `target_table`
- `keys`

### Supported CDC flows

- `auto_cdc`
  - uses `create_auto_cdc_flow_local`
  - requires `sequence_by`
  - produces an incremental merge flow with `dp.create_auto_cdc_flow`

- `auto_cdc_from_snapshot`
  - uses `create_auto_cdc_from_snapshot_flow_local`
  - requires `order_keys`
  - supports optional `track_history_column_list` or `track_history_except_column_list`

## Gold layer

Gold operations are expected to reference a SQL file path in `parameters['sql_file_path']`.

The file content is read and converted into Python code using `convert_sql_to_python_decorators(sql_content)`.

The converter accepts SQL statements in the form:

```sql
CREATE OR REFRESH MATERIALIZED VIEW schema.table AS (
  SELECT ...
);
```

and generates Python functions decorated with `@dp.materialized_view(...)`.

## Utility helpers

`dlt_creator/dlt_creator/utils/utils.py` defines type and schema helpers for PySpark:

- `spark_type(type_str, default_decimal=(38, 2))`
- `parse_type(type_obj, default_decimal=(38, 2))`
- `convert_struct_schema_string_to_struct_type(schema_json, default_decimal=(38, 2))`

These helpers support:

- primitive type aliases like `int`, `string`, `bool`
- nested `array` and `struct` definitions
- decimal precision/scale parsing

## Example metadata parameter payloads

### Bronze operation example

```json
{
  "target_table": "bronze.weather_data",
  "source_path": "/mnt/raw/weather",
  "schema": {
    "type": "struct",
    "fields": [
      {"name":"station_id","type":"string","nullable":false},
      {"name":"temperature","type":"double","nullable":true}
    ]
  },
  "file_format": "json"
}
```

### Silver operation example

```json
{
  "source_table": "bronze.weather_data",
  "target_table": "silver.weather_data",
  "cdc_parameters": {
    "cdc_type": "auto_cdc",
    "keys": ["station_id"],
    "sequence_by": "updated_at",
    "scd_type": 2
  }
}
```

### Gold operation example

```json
{
  "sql_file_path": "/Workspace/Repos/.../gold_views.sql"
}
```

## Extending the package

To add new behaviors:

1. Extend `transformation.runner()` with new `operation_type` handling.
2. Add supporting functions in `dlt_creator/utils/utils.py` or `dlt_creator/dlt_creator/transformation.py`.
3. Update metadata shape and job configuration accordingly.

## Notes

- The package is designed to work with Databricks and PySpark pipelines.
- Ensure `meteo_open_data.operations.metadata` contains valid, active operations before running.
- SQL-to-Python conversion is currently limited to `CREATE OR REFRESH MATERIALIZED VIEW ... AS (...)` patterns.
