import pytest
import os
import json
import pandas as pd
from engine.profile import profile_dataset

def test_profile_dataset_parquet(tmp_path):
    # Create prior version dir with data.parquet
    prior_dir = tmp_path / "prior"
    prior_dir.mkdir()
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df.to_parquet(prior_dir / "data.parquet")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    configs = {}
    dataset_dir = "dataset"
    profile_dataset(dataset_dir, configs, str(run_dir), str(prior_dir))
    out = json.loads((run_dir / "prior_profile.json").read_text())
    assert out["row_count"] == 2
    assert out["column_count"] == 2
    assert set(out["columns"].keys()) == {"a", "b"}

def test_profile_dataset_csv(tmp_path):
    prior_dir = tmp_path / "prior"
    prior_dir.mkdir()
    df = pd.DataFrame({"x": [5, 6, 7]})
    df.to_csv(prior_dir / "data.csv", index=False)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    configs = {}
    dataset_dir = "dataset"
    profile_dataset(dataset_dir, configs, str(run_dir), str(prior_dir))
    out = json.loads((run_dir / "prior_profile.json").read_text())
    assert out["row_count"] == 3
    assert out["column_count"] == 1
    assert "x" in out["columns"]

def test_profile_dataset_missing_file(tmp_path):
    prior_dir = tmp_path / "prior"
    prior_dir.mkdir()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    configs = {}
    dataset_dir = "dataset"
    with pytest.raises(FileNotFoundError):
        profile_dataset(dataset_dir, configs, str(run_dir), str(prior_dir))
