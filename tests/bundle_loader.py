"""Helpers that read the Databricks Asset Bundle definition for the test suite."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUNDLE_FILE = PROJECT_ROOT / "databricks.yml"
VARIABLE_PATTERN = re.compile(r"\$\{var\.([A-Za-z0-9_]+)\}")


def load_yaml(path: Path) -> dict:
    """Parse a YAML file, treating an empty document as an empty mapping."""
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def load_bundle() -> dict:
    """Return the parsed databricks.yml."""
    return load_yaml(BUNDLE_FILE)


def included_resource_files() -> list[Path]:
    """Resolve the bundle's include globs to the resource files they actually match."""
    files: list[Path] = []
    for pattern in load_bundle().get("include", []):
        files.extend(sorted(PROJECT_ROOT.glob(pattern)))
    return files


def load_resources() -> dict:
    """Merge every included resource file into one {type: {key: definition}} mapping."""
    merged: dict = {}
    for path in included_resource_files():
        for resource_type, definitions in load_yaml(path).get("resources", {}).items():
            merged.setdefault(resource_type, {}).update(definitions or {})
    return merged


def referenced_variables(node) -> set:
    """Collect every ${var.x} name referenced anywhere inside a nested structure."""
    if isinstance(node, dict):
        return set().union(*(referenced_variables(value) for value in node.values())) if node else set()
    if isinstance(node, list):
        return set().union(*(referenced_variables(item) for item in node)) if node else set()
    if isinstance(node, str):
        return set(VARIABLE_PATTERN.findall(node))
    return set()


def task_by_key(job: dict, task_key: str) -> dict:
    """Return the task definition with the given key, or fail if the job does not define it."""
    for task in job.get("tasks", []):
        if task.get("task_key") == task_key:
            return task
    raise AssertionError(f"job does not define task_key={task_key!r}")
