import os
import json
import pandas as pd
import numpy as np

def evaluate_dataset(dataset_dir, configs, run_dir):
    """
    Compares current run data to prior_profile.json, computes quality and drift metrics, outputs a deterministic JSON report.
    No thresholds, no pass/fail, no schema enforcement. Deterministic ordering and rounding.
    """
    # 1. Locate current data
    data_path = None
    for fname in ["data.parquet", "data.csv"]:
        fpath = os.path.join(run_dir, fname)
        if os.path.isfile(fpath):
            data_path = fpath
            break
    if data_path is None:
        raise FileNotFoundError(f"No generated data file found in {run_dir} (searched for data.parquet, data.csv)")

    # 2. Locate prior profile (may be missing on first run)
    prior_profile_path = os.path.join(run_dir, "prior_profile.json")
    prior_profile = None
    if os.path.isfile(prior_profile_path):
        with open(prior_profile_path, "r", encoding="utf-8") as f:
            prior_profile = json.load(f)

    # 3. Load data
    try:
        if data_path.endswith(".parquet"):
            df = pd.read_parquet(data_path)
        elif data_path.endswith(".csv"):
            df = pd.read_csv(data_path)
        else:
            raise ValueError("Unsupported data file type for evaluation.")
    except Exception as e:
        raise RuntimeError(f"Failed to read generated data file: {data_path}") from e

    # 4. Compute quality metrics (completeness, basic distribution sanity)
    quality = {}
    row_count = int(df.shape[0])
    column_count = int(df.shape[1])
    for col in sorted(df.columns):
        series = df[col]
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
        quality[col] = {
            "missing_ratio": missing_ratio,
            "cardinality": cardinality,
            "stats": stats
        }

    # 5. Compute drift metrics (statistical distance only, no decisions)
    drift = {}
    dataset_drift = {"row_count_drift": None, "column_count_drift": None}
    prior_cols = {}
    prior_row_count = None
    prior_column_count = None
    if prior_profile is not None:
        prior_cols = prior_profile.get("columns", {})
        prior_row_count = prior_profile.get("row_count")
        prior_column_count = prior_profile.get("column_count")
        dataset_drift["row_count_drift"] = abs(row_count - prior_row_count) if prior_row_count is not None else None
        dataset_drift["column_count_drift"] = abs(column_count - prior_column_count) if prior_column_count is not None else None
    for col in sorted(df.columns):
        drift[col] = {}
        if prior_profile is not None and col in prior_cols:
            # Numeric drift: compare means and stds (L1 distance) only if both are not None
            cur_stats = quality[col]["stats"]
            prior_stats = prior_cols[col]["stats"]
            for stat in ["mean", "std", "min", "max"]:
                cur_val = cur_stats[stat]
                prior_val = prior_stats.get(stat)
                if cur_val is not None and prior_val is not None:
                    drift_val = round(abs(cur_val - prior_val), 6)
                else:
                    drift_val = None
                drift[col][f"{stat}_drift"] = drift_val
            # Missingness drift
            cur_miss = quality[col]["missing_ratio"]
            prior_miss = prior_cols[col].get("missing_ratio")
            if prior_miss is not None:
                drift[col]["missing_ratio_drift"] = round(abs(cur_miss - prior_miss), 6)
            else:
                drift[col]["missing_ratio_drift"] = None
            # Cardinality drift
            cur_card = quality[col]["cardinality"]
            prior_card = prior_cols[col].get("cardinality")
            if prior_card is not None:
                drift[col]["cardinality_drift"] = abs(cur_card - prior_card)
            else:
                drift[col]["cardinality_drift"] = None
        else:
            # No prior stats for this column or no prior profile
            for stat in ["mean", "std", "min", "max"]:
                drift[col][f"{stat}_drift"] = None
            drift[col]["missing_ratio_drift"] = None
            drift[col]["cardinality_drift"] = None

    # 6. Output deterministic, machine-readable JSON report
    report = {
        "row_count": row_count,
        "column_count": column_count,
        "dataset_drift": dataset_drift,
        "quality": quality,
        "drift": drift
    }
    out_path = os.path.join(run_dir, "evaluation_report.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
    except Exception as e:
        raise RuntimeError(f"Failed to write evaluation report: {out_path}") from e