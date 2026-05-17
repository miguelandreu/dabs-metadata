# Medallion Asset Bundles

A multi-package repository for building, executing, and deploying medallion-style asset bundles with Databricks.

## Overview

This repository is organized into four main components:

- `api_executor/` — reusable library for API executions.
- `dlt_creator/` — utilities and flows for building Databricks DLT pipelines.
- `medallion_deployment/` — deployment and Databricks bundle configuration for the medallion asset pipeline.
- `infrastructure/` - includes the creation of the databricks infrastructure by using databricks asset bundles to deploy.

## Repository Structure

```text
api_executor/
  pyproject.toml
  README.md
  api_executor/
    __init__.py
    Requester.py

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

medallion_deployment/
  databricks.yml
  output.txt
  pyproject.toml
  README.md
  config/
    config.json
  resources/
    __init__.py

infrastructure/
  databricks.yml
  README.md
  sql_files/
    sql_creation.py
```

## Getting Started

### Prerequisites

- Python 3.11+ for `api_executor` and `dlt_creator`
- Python 3.10–3.12 for `medallion_deployment`
- Databricks CLI / Bundles installed for deployment and validation
- Access to the target Databricks workspace and required credentials

### Install Dependencies

Each package has its own `pyproject.toml` and can be installed independently.

Example:

```bash
cd api_executor
pip install -e .

cd ../dlt_creator
pip install -e .

cd ../medallion_deployment
pip install -e .
```

## Subproject Details

### `api_executor`

A small library for making API requests and handling execution logic.

- Main package: `api_executor`
- Entry points and helpers in `api_executor/Requester.py`
- Library can be build by using `uv build`.

### `dlt_creator`

Contains logic to build Databricks DLT pipelines and transformation utilities.

- Entry point: `dlt_creator/main.py`
- Transformations in `dlt_creator/transformation.py`
- Shared utilities in `dlt_creator/utils/utils.py`
- Library can be build by using `uv build`.

### `medallion_deployment`

Databricks bundle deployment configuration and packaging.

- Deployment config: `medallion_deployment/databricks.yml`
- Package metadata: `medallion_deployment/pyproject.toml`
- Main CLI entrypoint: `medallion_deployment.main:main`

### `infastructure`

Databricks bundle infrastructure deployment.

- Deployment config: `infrastructure/databricks.yml`

### How to Build the demo

0. **You need to install databricks-cli and have a databricks acount created.**
1. You can use the repository infrastructure deployment to deploy your infrastructure by using databricks asset bundle.
- Navigate to the project `infrastructure`.
- Create a virtual environment named .venv. Activate it.
- Note that you need to configure your workspace within `databricks.yml` file.
- Execute `databricks bundle deploy --profile {your_profile}`
    - If process fails due to free account, you can create it by hand.
    - If you need to indicate a storage account you can refer to https://www.linkedin.com/pulse/building-your-architecture-databricks-asset-bundles-andreu-nieva-kf9le/
2. When infrastructure created you can create the folders that allocates different elements:
- In `operations/metadata_storage` you need to create one folder for `libraries` and another folder for `gold_layer`.
- Use the file `sql_files\sql_creation.py` to create the tables and populate them with the examples.
3. Now you need to create the libraries and push them to the path `/Volumes/meteo_open_data/operations/metadata_storage/libraries/`, for that:
- Go to any of the projects:
    - `api_executor/`
    - `dlt_creator/`
- Create a virtual environment named .venv. Activate it.
- You can use `python build` my recommendation is to install `uv` and execute `uv build`.
- Copy the `.whl` file in the explained path.
4. Navigate to medallion_deployment to deploy the project:
- Create a virtual environment named .venv. Activate it.
- Install `uv` by executing `pip install uv`
- Synchroniza dependencies by `uv sync`.
- Deploy your project by executing `databricks bundle deploy --profile {your_profile}.
- Copy the file from `gold_sql_files/tenerife_meteo_open_gold_layer.sql` into the path `operations/metadata_storage/gold_layer`
5. Your code should be good to be executed now in the UI.
  

