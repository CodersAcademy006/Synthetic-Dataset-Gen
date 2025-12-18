import subprocess
import sys
import os
import json
import shutil
import tempfile
import pytest
from pathlib import Path

def test_runpy_finance_transactions(tmp_path):
    # Copy registry file
    registry_src = Path(__file__).parent.parent / "registry" / "datasets.json"
    registry_dst_dir = tmp_path / "registry"
    registry_dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(registry_src, registry_dst_dir / "datasets.json")
    # Copy finance_transactions dataset to tmp_path
    import_dir = Path(__file__).parent.parent / "datasets" / "finance_transactions"
    dataset_dir = tmp_path / "datasets" / "finance_transactions"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    for fname in ["dataset.yaml", "schema.yaml", "evolution.yaml"]:
        shutil.copy(import_dir / fname, dataset_dir / fname)
    # Prepare runs dir
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()
    # Run orchestrator
    script = Path(__file__).parent.parent / "scripts" / "run.py"
    # Set PYTHONPATH to project root so 'engine' can be imported
    env = os.environ.copy()
    project_root = str(Path(__file__).parent.parent)
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
    env["SYNTH_DATA_PROJECT_ROOT"] = str(tmp_path)
    result = subprocess.run([
        sys.executable, str(script),
        "--dataset", "finance_transactions"
    ], cwd=tmp_path, capture_output=True, text=True, env=env)
    assert result.returncode == 0, f"run.py failed: {result.stderr}"
    # Find run dir
    out_dirs = list((runs_dir / "finance_transactions").iterdir())
    assert out_dirs, "No run directory created"
    run_dir = out_dirs[0]
    # Check expected artifacts
    expected = [
        "configs_snapshot.json",
        "run_metadata.json",
        "validation_report.json",
        "evaluation_report.json",
        "final_metadata.json"
    ]
    for fname in expected:
        assert (run_dir / fname).exists(), f"Missing artifact: {fname}"
    # Data file
    assert (run_dir / "data.parquet").exists() or (run_dir / "data.csv").exists(), "Missing data file"
