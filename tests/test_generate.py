import pytest
import os
import pandas as pd
import numpy as np
import json
from engine.generate import generate_dataset

def minimal_configs():
    return {
        "dataset.yaml": {"row_count": 10},
        "schema.yaml": {"columns": ["id", "amount"]},
        "evolution.yaml": {"fraud_rate": 0.0, "missingness": {}}
    }

def test_generate_dataset_parquet(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    dataset_dir = "dataset"
    configs = minimal_configs()
    generate_dataset(dataset_dir, configs, str(run_dir))
    # Should write data.parquet or data.csv
    files = list(run_dir.iterdir())
    assert any(f.name == "data.parquet" or f.name == "data.csv" for f in files)
    # Data shape
    if (run_dir / "data.parquet").exists():
        df = pd.read_parquet(run_dir / "data.parquet")
    else:
        df = pd.read_csv(run_dir / "data.csv")
    assert df.shape == (10, 2)
    assert set(df.columns) == {"id", "amount"}

def test_generate_dataset_determinism(tmp_path):
    run_dir1 = tmp_path / "run1"
    run_dir2 = tmp_path / "run2"
    run_dir1.mkdir()
    run_dir2.mkdir()
    dataset_dir = "dataset"
    configs = minimal_configs()
    generate_dataset(dataset_dir, configs, str(run_dir1))
    generate_dataset(dataset_dir, configs, str(run_dir2))
    if (run_dir1 / "data.parquet").exists():
        df1 = pd.read_parquet(run_dir1 / "data.parquet")
        df2 = pd.read_parquet(run_dir2 / "data.parquet")
    else:
        df1 = pd.read_csv(run_dir1 / "data.csv")
        df2 = pd.read_csv(run_dir2 / "data.csv")
    pd.testing.assert_frame_equal(df1, df2)
