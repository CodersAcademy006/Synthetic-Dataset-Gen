def persist_artifacts(dataset_dir, configs, run_dir):
    import os
    import json
    from pathlib import Path
    from datetime import datetime, timezone

    run_dir = Path(run_dir)
    final_metadata_path = run_dir / "final_metadata.json"
    if final_metadata_path.exists():
        raise RuntimeError(f"Run is already finalized: {final_metadata_path}")

    # Required logical artifacts
    required_artifacts = [
        "configs_snapshot.json",
        "run_metadata.json",
        "validation_report.json",
        "evaluation_report.json"
    ]

    # Data file: exactly one of data.parquet or data.csv
    data_parquet = run_dir / "data.parquet"
    data_csv = run_dir / "data.csv"
    has_parquet = data_parquet.exists()
    has_csv = data_csv.exists()
    if has_parquet and has_csv:
        raise RuntimeError("Ambiguous data files: both data.parquet and data.csv exist")
    if not has_parquet and not has_csv:
        raise RuntimeError("Missing data file: neither data.parquet nor data.csv exist")
    data_file = "data.parquet" if has_parquet else "data.csv"

    # Check all required artifacts exist
    for fname in required_artifacts:
        if not (run_dir / fname).is_file():
            raise RuntimeError(f"Missing required artifact: {fname}")

    # configs_snapshot.json must match provided configs (deep equality)
    with open(run_dir / "configs_snapshot.json", "r", encoding="utf-8") as f:
        snapshot = json.load(f)
    if snapshot != configs:
        raise RuntimeError("configs_snapshot.json does not match provided configs")

    # Compose manifest (deterministic key order)
    manifest = {
        "dataset": os.path.basename(os.path.normpath(dataset_dir)),
        "run_dir": str(run_dir.resolve()),
        "finalized_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "artifacts": {
            "data": data_file,
            "configs_snapshot": "configs_snapshot.json",
            "run_metadata": "run_metadata.json",
            "validation_report": "validation_report.json",
            "evaluation_report": "evaluation_report.json"
        }
    }

    # Write manifest atomically (no partial writes)
    tmp_path = final_metadata_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, final_metadata_path)