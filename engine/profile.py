# Descriptive profiling for prior version data (production baseline)
import os
import sys
import json
import pandas as pd
import numpy as np

def profile_dataset(dataset_dir, configs, run_dir, prior_version_dir):
    """
    Profiles the prior version's data and writes a deterministic JSON summary to run_dir/prior_profile.json.
    Raises on any error or missing data.

    Note: For CSVs, pandas dtype inference may not be stable across platforms or pandas versions.
    """
    # 1. Locate prior data file
    candidates = ["data.parquet", "data.csv"]
    prior_data_path = None
    for fname in candidates:
        fpath = os.path.join(prior_version_dir, fname)
        if os.path.isfile(fpath):
            prior_data_path = fpath
            break
    if prior_data_path is None:
        raise FileNotFoundError(
            f"No prior data file found in {prior_version_dir} (searched for: {candidates})"
        )

    # 2. Read data
    try:
        if prior_data_path.endswith(".parquet"):
            df = pd.read_parquet(prior_data_path)
        elif prior_data_path.endswith(".csv"):
            # WARNING: dtype inference for CSVs may not be stable across platforms or pandas versions.
            df = pd.read_csv(prior_data_path)
        else:
            raise ValueError("Unsupported file type for profiling.")
    except Exception as e:
        raise RuntimeError(f"Failed to read prior data file: {prior_data_path}") from e

    # 3. Compute dataset-level stats
    row_count = int(df.shape[0])
    column_count = int(df.shape[1])

    # 4. Compute per-column stats (deterministic order)
    columns = {}
    for col in sorted(df.columns):
        series = df[col]
        dtype = str(series.dtype)
        # Cardinality is defined as the number of distinct non-null values.
        missing_ratio = round(float(series.isna().sum()) / row_count, 6) if row_count > 0 else 0.0
        cardinality = int(series.nunique(dropna=True))
        stats = {
            "mean": None,
            "std": None,
            "min": None,
            "max": None
        }
        if np.issubdtype(series.dtype, np.number):
            if not series.isna().all():
                stats["mean"] = round(float(series.mean()), 6)
                stats["std"] = round(float(series.std()), 6)
                stats["min"] = round(float(series.min()), 6)
                stats["max"] = round(float(series.max()), 6)
        # For non-numeric, all stats remain None

        columns[col] = {
            "dtype": dtype,
            "missing_ratio": missing_ratio,
            "cardinality": cardinality,
            "stats": {
                "mean": stats["mean"],
                "std": stats["std"],
                "min": stats["min"],
                "max": stats["max"]
            }
        }

    # 5. Write output (deterministic, byte-stable)
    prior_version = os.path.basename(prior_version_dir)
    profile = {
        "source_version": prior_version,
        "row_count": row_count,
        "column_count": column_count,
        "columns": columns
    }
    out_path = os.path.join(run_dir, "prior_profile.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(profile, f, indent=2, sort_keys=False, ensure_ascii=False)
    except Exception as e:
        raise RuntimeError(f"Failed to write profile output: {out_path}") from e
