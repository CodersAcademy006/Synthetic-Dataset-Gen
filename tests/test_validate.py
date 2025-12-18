import pytest
import os
import pandas as pd
import json
from engine.validate import validate_dataset

def minimal_configs():
    return {
        "dataset.yaml": {"row_count": 5},
        "schema.yaml": {"columns": ["id", "amount"]},
        "evolution.yaml": {"fraud_rate": 0.0, "missingness": {}}
    }

def test_validate_dataset_pass(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "amount": [10, 20, 30, 40, 50]})
    df.to_parquet(run_dir / "data.parquet")
    configs = minimal_configs()
    validate_dataset("dataset", configs, str(run_dir))
    report = json.loads((run_dir / "validation_report.json").read_text())
    assert report["status"] == "pass"
    assert report["row_count"] == 5
    assert report["column_count"] == 2

def test_validate_dataset_missing_column(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5]})
    df.to_parquet(run_dir / "data.parquet")
    configs = minimal_configs()
    with pytest.raises(ValueError):
        validate_dataset("dataset", configs, str(run_dir))
