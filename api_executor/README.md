# api_executor

A lightweight Python package for fetching data from REST APIs and persisting JSON payloads to a file path accessible from Databricks.

This package is intended to be used as part of medallion-style asset ingestion workflows, where raw API responses are captured and persisted before downstream transformation.

## Features

- Query a REST API endpoint and validate HTTP `200` responses
- Extract the payload from the JSON body using a clean endpoint key
- Save output files with timestamped filenames
- Support both independent endpoints and dependent endpoint flows
- Designed to run in a PySpark / Databricks environment using `dbutils`

## Package structure

```text
api_executor/
  pyproject.toml
  README.md
  api_executor/
    __init__.py
    Requester.py
```

## Requirements

- Python 3.11+
- `requests`
- `pyspark`

The package declares `requests` in `api_executor/pyproject.toml`.

## How it works

### `Requester`

The `Requester` class encapsulates API retrieval logic and file persistence.

Constructor parameters:

- `base_url: str` — root URL for the API.
- `base_volume_path: str` — target directory where JSON files are written.
- `endpoint: str` — API path to query.

Key methods:

- `get_request(endpoint: str = None, clean_key: str = None) -> dict`
- `save_file(response: dict) -> None`
- `independent_api()`
- `dependent_api(dependencies)`

### `runner(operation)`

A helper function that loads parameters from a task `operation` dictionary and executes either `independent_api` or `dependent_api`.

The operation should include a JSON string under `parameters` with keys:

- `base_url`
- `base_volume_path`
- `endpoint`
- `dependencies` (optional)

## Usage examples

### Basic independent API request

```python
from api_executor.Requester import Requester

requester = Requester(
    base_url="https://api.example.com",
    base_volume_path="/dbfs/mnt/raw/api",
    endpoint="/stations"
)

response = requester.get_request()
requester.save_file(response)
```

### Run the full independent flow

```python
from api_executor.Requester import Requester

requester = Requester(
    base_url="https://api.example.com",
    base_volume_path="/dbfs/mnt/raw/api",
    endpoint="/stations"
)

requester.independent_api()
```

### Dependent API flow

Use `dependent_api` when the requested resource requires a prior dependency list.

For example, the package currently supports:

- `sensors` requiring station endpoints
- `readings` requiring station endpoints and date parameters

```python
from api_executor.Requester import Requester

dependencies = [
    {"endpoint": "/stations"}
]

requester = Requester(
    base_url="https://api.example.com",
    base_volume_path="/dbfs/mnt/raw/api",
    endpoint="/sensors/{idWeatherstation}"
)

requester.dependent_api(dependencies)
```

### Using `runner(operation)`

```python
from api_executor.Requester import runner
import json

operation = {
    "parameters": json.dumps({
        "base_url": "https://api.example.com",
        "base_volume_path": "/dbfs/mnt/raw/api",
        "endpoint": "/stations"
    })
}

runner(operation)
```

## API behavior

### Endpoint cleaning

`Requester` derives a `clean_key` from the endpoint path:

- `/stations` → `stations`
- `/sensors/{idWeatherstation}` → `sensors`

This key is used to extract the relevant list from the API response and to build the output directory.

### Response extraction

The method `__extract_value_from_response__` expects a JSON payload shaped like:

```json
{
  "stations": [ ... ]
}
```

It returns the list stored under the endpoint key or an empty list if the key is not found.

### Output file format

`save_file()` writes a timestamped JSON file to:

```
{base_volume_path}/{clean_key}/{clean_key}_{YYYYMMDD_HHMMSS}.json
```

Example:

```
/dbfs/mnt/raw/api/stations/stations_20250515_101530.json
```

## Notes and best practices

- `Requester` uses `dbutils.fs.mkdirs(...)` to create the output folder. Ensure the target path is accessible in your runtime.
- The package assumes the API returns JSON and uses the `accept: application/json` header.
- Failure status codes raise an exception with the response body.
- The current implementation supports a limited dependent flow for `sensors` and `readings` endpoints. Extend `dependent_api()` if you need additional dependency patterns.

## Extending the package

To add new dependency workflows:

1. Add a condition in `Requester.dependent_api()` for the new `clean_key`.
2. Use `self.get_request(...)` to resolve prerequisite endpoints.
3. Build the final response list.
4. Call `self.save_file(response)`.
