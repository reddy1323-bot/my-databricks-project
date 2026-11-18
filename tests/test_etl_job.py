"""Tests for the etl_flow job definition and the notebooks it schedules."""

from pathlib import Path

from bundle_loader import PROJECT_ROOT, load_resources, task_by_key

JOB_KEY = "etl_flow"
EXPECTED_CHAIN = [
    ("landing_to_feed", None),
    ("feed_to_stg", "landing_to_feed"),
    ("stg_to_datamart", "feed_to_stg"),
]
LAYER_PARAMETERS = {
    "landing_to_feed": {"catalog", "schema", "landing_path", "entity", "run_date"},
    "feed_to_stg": {"catalog", "schema", "entity", "business_key", "run_date"},
    "stg_to_datamart": {"catalog", "schema", "entity", "business_key", "run_date"},
}


def etl_job() -> dict:
    """Return the etl_flow job definition."""
    jobs = load_resources()["jobs"]
    assert JOB_KEY in jobs, f"bundle does not define the {JOB_KEY} job"
    return jobs[JOB_KEY]


def test_job_declares_the_three_layer_chain_in_order():
    tasks = etl_job()["tasks"]

    assert [task["task_key"] for task in tasks] == [key for key, _ in EXPECTED_CHAIN]
    for task_key, upstream in EXPECTED_CHAIN:
        task = task_by_key(etl_job(), task_key)
        dependencies = [dependency["task_key"] for dependency in task.get("depends_on", [])]
        assert dependencies == ([upstream] if upstream else []), task_key


def test_every_task_passes_the_parameters_its_notebook_reads():
    for task_key, expected_parameters in LAYER_PARAMETERS.items():
        task = task_by_key(etl_job(), task_key)
        passed = set(task["notebook_task"]["base_parameters"])

        assert passed == expected_parameters, f"{task_key} parameters: {sorted(passed)}"


def test_notebook_paths_resolve_to_files_in_the_repository():
    resource_file = next(
        path
        for path in (PROJECT_ROOT / "databricks" / "resources").glob("*.yml")
        if JOB_KEY in path.read_text(encoding="utf-8")
    )

    for task in etl_job()["tasks"]:
        notebook_path = task["notebook_task"]["notebook_path"]
        resolved = (resource_file.parent / notebook_path).with_suffix(".py")
        assert resolved.is_file(), f"{task['task_key']} points at missing notebook {resolved}"


def test_each_task_runs_on_the_shared_job_cluster():
    job = etl_job()
    cluster_keys = {cluster["job_cluster_key"] for cluster in job.get("job_clusters", [])}

    assert cluster_keys, "job defines no job_clusters"
    for task in job["tasks"]:
        assert task.get("job_cluster_key") in cluster_keys, task["task_key"]


def test_notebooks_are_databricks_source_files():
    notebooks = sorted((PROJECT_ROOT / "src" / "etl").glob("*.py"))

    assert notebooks, "src/etl contains no notebooks"
    for notebook in notebooks:
        header = Path(notebook).read_text(encoding="utf-8").splitlines()[0]
        assert header == "# Databricks notebook source", notebook.name
