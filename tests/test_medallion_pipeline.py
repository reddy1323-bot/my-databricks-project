import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "src" / "pipelines" / "medallion_pipeline.py"


def test_pipeline_source_is_valid_python():
    source = PIPELINE.read_text(encoding="utf-8")
    ast.parse(source)


def test_pipeline_defines_all_medallion_tables_and_quality_rules():
    source = PIPELINE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    table_names = {
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr == "table"
        for keyword in decorator.keywords
        if keyword.arg == "name"
        and isinstance(keyword.value, ast.Constant)
    }

    assert table_names == {
        "bronze_sales_events",
        "silver_sales_events",
        "gold_daily_customer_sales",
    }
    assert "valid_event_id" in PIPELINE.read_text(encoding="utf-8")
    assert "valid_event_timestamp" in PIPELINE.read_text(encoding="utf-8")
    assert "valid_customer_id" in PIPELINE.read_text(encoding="utf-8")
    assert "valid_event_type" in PIPELINE.read_text(encoding="utf-8")
    assert "valid_nonnegative_amount" in PIPELINE.read_text(encoding="utf-8")
    assert '"cloudFiles"' in source
    assert 'F.sum("amount").alias("gross_sales")' in source
    assert 'F.count("*").alias("event_count")' in source


def test_prod_target_uses_production_mode_and_separate_schema_state():
    bundle = (ROOT / "databricks.yml").read_text(encoding="utf-8")

    assert "schema: medallion_prod" in bundle
    assert "default: demo" in bundle
    assert "default: /Volumes/demo/feed/feed" in bundle
    assert "default: /Volumes/demo/feed/feed/_autoloader/sales_events_dev" in bundle
    assert (
        "schema_location: /Volumes/demo/feed/feed/_autoloader/sales_events_prod"
        in bundle
    )
    assert "development: false" in bundle


def test_bundle_includes_pipeline_resource_file():
    bundle = (ROOT / "databricks.yml").read_text(encoding="utf-8")
    resource = ROOT / "databricks" / "resources" / "medallion_pipeline.yml"

    assert "databricks/resources/*.yml" in bundle
    assert resource.is_file()
    assert "medallion_pipeline:" in resource.read_text(encoding="utf-8")
