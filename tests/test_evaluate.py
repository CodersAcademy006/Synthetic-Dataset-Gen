import pytest
import os
import pandas as pd
import numpy as np
import json
from engine.evaluate import evaluate_dataset

def test_evaluate_dataset_basic(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df.to_parquet(run_dir / "data.parquet")
    configs = {}
    dataset_dir = "dataset"
    evaluate_dataset(dataset_dir, configs, str(run_dir))
    report = json.loads((run_dir / "evaluation_report.json").read_text())
    assert report["row_count"] == 3
    assert report["column_count"] == 2
    assert set(report["quality"].keys()) == {"a", "b"}
    assert set(report["drift"].keys()) == {"a", "b"}

def test_evaluate_dataset_with_prior_profile(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    df = pd.DataFrame({"a": [1, 2, 3]})
    df.to_parquet(run_dir / "data.parquet")
    prior_profile = {
        "source_version": "v0",
        "row_count": 3,
        "column_count": 1,
        "columns": {
            "a": {
                "dtype": "int64",
                "missing_ratio": 0.0,
                "cardinality": 3,
                "stats": {"mean": 2.0, "std": 1.0, "min": 1.0, "max": 3.0}
            }
        }
    }
    (run_dir / "prior_profile.json").write_text(json.dumps(prior_profile))
    configs = {}
    dataset_dir = "dataset"
    evaluate_dataset(dataset_dir, configs, str(run_dir))
    report = json.loads((run_dir / "evaluation_report.json").read_text())
    assert report["row_count"] == 3
    assert report["drift"]["a"]["mean_drift"] == 0.0
