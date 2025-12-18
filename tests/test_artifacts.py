import pytest
import os
import json
from engine.artifacts import persist_artifacts

def make_artifacts(tmp_path, configs):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # Required files
    (run_dir / "configs_snapshot.json").write_text(json.dumps(configs))
    (run_dir / "run_metadata.json").write_text("{}")
    (run_dir / "validation_report.json").write_text("{}")
    (run_dir / "evaluation_report.json").write_text("{}")
    (run_dir / "data.parquet").write_bytes(b"parquetdata")
    return run_dir

def test_persist_artifacts_success(tmp_path):
    configs = {"a": 1}
    run_dir = make_artifacts(tmp_path, configs)
    persist_artifacts("/dataset", configs, str(run_dir))
    meta = json.loads((run_dir / "final_metadata.json").read_text())
    assert meta["artifacts"]["data"] == "data.parquet"
    assert meta["artifacts"]["configs_snapshot"] == "configs_snapshot.json"
    assert meta["run_dir"] == str(run_dir.resolve())
    assert "finalized_at_utc" in meta

def test_persist_artifacts_raises_on_refinalize(tmp_path):
    configs = {"a": 1}
    run_dir = make_artifacts(tmp_path, configs)
    persist_artifacts("/dataset", configs, str(run_dir))
    with pytest.raises(RuntimeError):
        persist_artifacts("/dataset", configs, str(run_dir))

def test_persist_artifacts_missing_file(tmp_path):
    configs = {"a": 1}
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # Only some files
    (run_dir / "configs_snapshot.json").write_text(json.dumps(configs))
    (run_dir / "run_metadata.json").write_text("{}")
    # Missing validation_report.json
    (run_dir / "evaluation_report.json").write_text("{}")
    (run_dir / "data.parquet").write_bytes(b"parquetdata")
    with pytest.raises(RuntimeError):
        persist_artifacts("/dataset", configs, str(run_dir))
