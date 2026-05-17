# infrastructure

This folder contains the Databricks asset bundle configuration and helper notebook for medallion asset deployment.

The main purpose of this package is to provision the catalog, schemas, and volumes needed for the asset ingestion pipeline and to initialize metadata used by the DLT pipeline orchestrator.

## Contents

- `databricks.yml` — Databricks asset bundle definition for catalog, schema, and volume resources.
- `sql_files/sql_creation.py` — helper notebook script to create metadata tables and seed initial operation configuration.

## Databricks bundle configuration

The bundle defines:

- `meteo_open_data` catalog
- medallion layer schemas:
  - `landing`
  - `bronze`
  - `silver`
  - `gold`
- metadata schema:
  - `operations`
- volumes:
  - `raw_api_landing`
  - `metadata_storage`

### Deployment targets

- `dev` — development mode with workspace-prefixed resources
- `prod` — production mode with production resource permissions

### Usage

1. Authenticate to your Databricks workspace:

```bash
databricks configure --token
```

2. Validate the bundle locally if desired:

```bash
databricks bundle validate --profile <profile>
```

3. Deploy the bundle:

```bash
databricks bundle deploy --target dev
```

or

```bash
databricks bundle deploy --target prod
```

## Metadata initialization

The helper notebook in `sql_files/sql_creation.py` is used to:

- create the metadata table `meteo_open_data.operations.metadata`
- insert API ingestion metadata for raw API endpoints
- insert bronze pipeline metadata for `measures` and `stations`

### metadata table schema

The metadata table stores:

- `operation_name`
- `ingestion_name`
- `operation_type`
- `parameters`
- `is_active`
- `creation_date`

The `parameters` column contains JSON-serialized configuration used by the pipeline runner.

### Example seeded operations

The current notebook seeds operations such as:

- API ingestion for `/measures` and `/stations`
- dependent API ingestion for `/sensors` and `/readings/summarized`
- Bronze table creation for `measures_tenerife` and `stations_tenerife`

## Notes

- The `databricks.yml` bundle is configured for direct engine deployment.
- Update the workspace host values in the `dev` and `prod` targets before deployment.
- Use `databricks bundle validate` to ensure the bundle syntax is correct for your Databricks environment.

## Contribution

Add or update metadata rows in `sql_files/sql_creation.py` when your pipeline operation definitions change.