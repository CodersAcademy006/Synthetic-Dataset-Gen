def ingest_external_dataset(input_path, run_dir):
    """
    Ingest an external dataset into the platform in a deterministic, auditable way.

    Notes:
    - If writing as CSV, pandas dtype inference may vary across platforms and pandas versions.
      This is a known limitation and is not fixed here by contract.
    """
    import os
    import shutil
    import pandas as pd
    from pathlib import Path

    input_path = Path(input_path)
    run_dir = Path(run_dir)

    # 1. Validate input_path
    if not input_path.exists() or not input_path.is_file():
        raise RuntimeError(f"input_path does not exist or is not a file: {input_path}")
    if input_path.suffix.lower() not in {".csv", ".parquet"}:
        raise RuntimeError("input_path must be a .csv or .parquet file")

    # 2. Validate run_dir
    if not run_dir.exists() or not run_dir.is_dir():
        raise RuntimeError(f"run_dir does not exist or is not a directory: {run_dir}")
    if any(run_dir.iterdir()):
        raise RuntimeError("run_dir must be empty")

    # 3. Load dataset
    try:
        if input_path.suffix.lower() == ".csv":
            df = pd.read_csv(input_path)
        else:
            df = pd.read_parquet(input_path)
    except Exception as e:
        raise RuntimeError(f"Failed to load dataset: {e}")

    if df.empty:
        raise RuntimeError("Input dataset is empty")

    # 4. Normalize: sort columns lexicographically, preserve row order
    df = df[sorted(df.columns)]

    # 5. Write output atomically
    out_parquet = run_dir / "data.parquet"
    out_csv = run_dir / "data.csv"
    tmp_path = None

    try:
        tmp_path = out_parquet.with_suffix(".tmp")
        df.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, out_parquet)
    except Exception:
        # Parquet failed, fall back to CSV
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()
        try:
            tmp_path = out_csv.with_suffix(".tmp")
            df.to_csv(tmp_path, index=False)
            os.replace(tmp_path, out_csv)
        except Exception as e:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()
            raise RuntimeError(f"Failed to write dataset as parquet or csv: {e}")