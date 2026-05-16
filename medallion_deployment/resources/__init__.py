import json
from pathlib import Path

from databricks.bundles.core import Bundle, Resources
from databricks.bundles.jobs import Job
from databricks.bundles.pipelines import Pipeline

# Constants
NOTEBOOK_BASE_PATH = "/Workspace/Shared/runner_notebooks"
API_EXECUTOR_LIBRARY_PATH = "/Volumes/meteo_open_data/operations/metadata_storage/libraries/api_executor-0.1.0-py3-none-any.whl"
MEDALLION_LIBRARY_PATH = "/Volumes/meteo_open_data/operations/metadata_storage/libraries/dlt_creator-0.1.0-py3-none-any.whl"
CONFIG_PATH = Path(__file__).parent.parent / "config" / "config.json"

# Bundle variable references — resolved by DABS at deploy time
CATALOG = "${var.catalog}"
SCHEMA = "${var.schema}"


def _load_config() -> list[dict]:
    with open(CONFIG_PATH) as f:
        return json.load(f)


def _build_tags(tags: list[dict]) -> dict[str, str]:
    """Flatten list of tag dicts into a single dict for Databricks job tags."""
    result = {}
    for tag in tags:
        result.update(tag)
    return result


def _create_pipeline(pipeline_group: str, tags: dict[str, str]) -> Pipeline:
    return Pipeline.from_dict({
        "name": f"pipeline_{pipeline_group}_dlt",
        "catalog": CATALOG,
        "target": SCHEMA,
        "tags": tags,
        "libraries": [
            {
                "notebook": {
                    "path": f"{NOTEBOOK_BASE_PATH}/dlt_runner",
                }
            },
        ],
        "configuration": {
            "operation_name": pipeline_group,
        },
        "environment": {
            "dependencies": [MEDALLION_LIBRARY_PATH],
        },
        "development": True,
        "serverless": True,
        "continuous": False,
        "photon": False,
    })


def _create_job(pipeline_group: str, tags: dict[str, str]) -> Job:
    pipeline_ref = f"${{resources.pipelines.pipeline_{pipeline_group}_dlt.id}}"

    return Job.from_dict({
        "name": f"pipeline_{pipeline_group}",
        "queue": {"enabled": True},
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "tags": tags,
        "environments": [
            {
                "environment_key": "api_executor_env",
                "spec": {
                    "environment_version": "2",
                    "dependencies": [API_EXECUTOR_LIBRARY_PATH],
                },
            }
        ],
        "tasks": [
            {
                "task_key": "get_pipeline_group_parameters",
                "notebook_task": {
                    "notebook_path": f"{NOTEBOOK_BASE_PATH}/audit_table_runner",
                    "source": "WORKSPACE",
                    "base_parameters": {
                        "operation_name": pipeline_group,
                    },
                },
            },
            {
                "task_key": "execute_api",
                "depends_on": [{"task_key": "get_pipeline_group_parameters"}],
                "for_each_task": {
                    "inputs": "{{tasks.get_pipeline_group_parameters.values.response}}",
                    "concurrency": 4,
                    "task": {
                        "task_key": "execute_api_iteration",
                        "environment_key": "api_executor_env",
                        "notebook_task": {
                            "notebook_path": f"{NOTEBOOK_BASE_PATH}/api_runner",
                            "source": "WORKSPACE",
                            "base_parameters": {
                                "response": "{{input}}",
                            },
                        },
                    },
                },
            },
            {
                "task_key": "medallion_architecture",
                "depends_on": [{"task_key": "execute_api"}],
                "pipeline_task": {
                    "pipeline_id": pipeline_ref,
                    "full_refresh": False,
                },
            },
        ],
    })


def load_resources(bundle: Bundle) -> Resources:
    config = _load_config()
    resources = Resources()

    for pipeline in config:
        pipeline_group = pipeline["pipeline_group"]
        tags = _build_tags(pipeline.get("tags", []))

        dlt_pipeline = _create_pipeline(pipeline_group=pipeline_group, tags=tags)
        job = _create_job(pipeline_group=pipeline_group, tags=tags)

        resources.add_pipeline(f"pipeline_{pipeline_group}_dlt", dlt_pipeline)
        resources.add_job(f"pipeline_{pipeline_group}", job)

    return resources