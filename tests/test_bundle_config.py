"""Tests for the bundle root: targets, includes and variable wiring."""

from bundle_loader import (
    included_resource_files,
    load_bundle,
    load_resources,
    referenced_variables,
)

EXPECTED_TARGETS = {"dev", "prod"}


def resource_definitions():
    """Yield every resource definition so variable references inside them are visible."""
    for definitions in load_resources().values():
        yield from definitions.values()


def test_bundle_declares_development_and_production_targets():
    targets = load_bundle()["targets"]

    assert set(targets) == EXPECTED_TARGETS
    assert targets["dev"]["mode"] == "development"
    assert targets["dev"].get("default") is True
    assert targets["prod"]["mode"] == "production"


def test_include_globs_resolve_to_resource_files():
    assert included_resource_files(), "include globs match no resource files"


def test_legacy_workspace_export_is_excluded_from_bundle_sync():
    assert "src/ETLPipeline.dbc" in load_bundle()["sync"]["exclude"]


def test_bundle_declares_at_least_one_job():
    resources = load_resources()

    assert resources, "bundle declares no resources, so deploy would be a no-op"
    assert resources.get("jobs"), "bundle declares no jobs"


def test_every_referenced_variable_is_declared():
    declared = set(load_bundle().get("variables", {}))
    referenced = referenced_variables(load_bundle()) | referenced_variables(
        list(resource_definitions())
    )

    assert referenced <= declared, f"undeclared variables: {sorted(referenced - declared)}"


def test_production_overrides_the_catalog():
    prod_variables = load_bundle()["targets"]["prod"].get("variables", {})

    assert prod_variables.get("catalog") == "prod_catalog"
