
import os
import json
import hashlib
import tempfile
from typing import Dict, List, Any

import numpy as np
import pandas as pd
import random


def _require_configs(configs: Dict[str, Any]) -> None:
	required = ["dataset.yaml", "schema.yaml", "evolution.yaml"]
	for key in required:
		if key not in configs or configs[key] is None:
			raise KeyError(f"Missing required config: {key}")


def _extract_columns(schema_cfg: Any) -> List[str]:
	# Columns must match schema.yaml names exactly; support mapping (preferred) or list fallback for legacy.
	if not isinstance(schema_cfg, dict) or "columns" not in schema_cfg:
		raise KeyError("schema.yaml must contain 'columns'")
	cols = schema_cfg["columns"]
	if isinstance(cols, dict):
		# Use mapping keys as column names, sorted for determinism
		names = sorted(cols.keys())
		if not names:
			raise ValueError("schema.yaml 'columns' mapping is empty")
		return names
	elif isinstance(cols, list):
		# Legacy fallback: list of names or dicts with 'name'
		names: List[str] = []
		for c in cols:
			if isinstance(c, str):
				names.append(c)
			elif isinstance(c, dict) and "name" in c:
				names.append(c["name"])
			else:
				raise ValueError("Invalid column entry in schema.yaml; expected string or dict with 'name'")
		if not names:
			raise ValueError("schema.yaml 'columns' list is empty")
		return names
	else:
		raise ValueError("schema.yaml 'columns' must be a mapping or a list")


def _extract_row_count(dataset_cfg: Any) -> int:
	# Row count must be config-driven; we use 'row_count' for clarity and explicitness.
	# This avoids hardcoded defaults and keeps generation scale controlled by config.
	if not isinstance(dataset_cfg, dict):
		raise KeyError("dataset.yaml must be a mapping")
	rc = dataset_cfg.get("row_count")
	if rc is None:
		# Allow 'rows' as an alternate explicit field, still config-driven.
		rc = dataset_cfg.get("rows")
	if rc is None:
		raise KeyError("dataset.yaml must define 'row_count' (or 'rows')")
	try:
		rc_int = int(rc)
	except Exception:
		raise ValueError("row_count must be an integer")
	if rc_int <= 0:
		raise ValueError("row_count must be > 0")
	return rc_int


def _extract_evolution(evo_cfg: Any) -> Dict[str, Any]:
	if not isinstance(evo_cfg, dict):
		raise KeyError("evolution.yaml must be a mapping")
	# Project convention: fraud_rate is defined in the first entry of 'weekly_changes' (if present)
	fraud_rate = None
	if "weekly_changes" in evo_cfg and isinstance(evo_cfg["weekly_changes"], list) and evo_cfg["weekly_changes"]:
		first_week = evo_cfg["weekly_changes"][0]
		if isinstance(first_week, dict) and "fraud_rate" in first_week:
			fraud_rate = first_week["fraud_rate"]
	elif "fraud_rate" in evo_cfg:
		fraud_rate = evo_cfg.get("fraud_rate")
	else:
		fraud = evo_cfg.get("fraud")
		if isinstance(fraud, dict):
			fraud_rate = fraud.get("rate")
	if fraud_rate is not None:
		try:
			fraud_rate = float(fraud_rate)
		except Exception:
			raise ValueError("fraud_rate must be numeric")
		if fraud_rate < 0.0 or fraud_rate > 1.0:
			raise ValueError("fraud_rate must be in [0,1]")

	missingness = evo_cfg.get("missingness")
	if missingness is not None and not isinstance(missingness, dict):
		raise ValueError("missingness must be a mapping of column_name -> ratio")
	if isinstance(missingness, dict):
		for k, v in missingness.items():
			try:
				r = float(v)
			except Exception:
				raise ValueError("missingness ratios must be numeric")
			if r < 0.0 or r > 1.0:
				raise ValueError("missingness ratio must be in [0,1]")

	return {"fraud_rate": fraud_rate, "missingness": missingness or {}}
def _deterministic_col_hash(name: str) -> int:
	# Use SHA-256 of column name for deterministic per-column RNG
	h = hashlib.sha256(name.encode("utf-8")).digest()
	return int.from_bytes(h[:4], byteorder="big", signed=False)


def _derive_seed(dataset_dir: str, run_dir: str) -> int:
	dataset_name = os.path.basename(os.path.normpath(dataset_dir))
	version = os.path.basename(os.path.normpath(run_dir))
	h = hashlib.sha256(f"{dataset_name}:{version}".encode("utf-8")).digest()
	# Use first 8 bytes as unsigned 64-bit, then fold to 32-bit for RandomState
	seed64 = int.from_bytes(h[:8], byteorder="big", signed=False)
	return seed64 % (2 ** 32)


def _apply_missingness(rs: np.random.RandomState, arr: np.ndarray, ratio: float) -> np.ndarray:
	if ratio is None or ratio <= 0.0:
		return arr
	n = arr.shape[0]
	k = int(round(ratio * n))
	if k <= 0:
		return arr
	idx = rs.permutation(n)[:k]
	# Assign missing values; for object dtype use None, for numeric use np.nan
	if arr.dtype.kind in ("f", "i", "u"):  # float/int/uint
		arr = arr.astype(float)
		arr[idx] = np.nan
	else:
		arr = arr.astype(object)
		for i in idx:
			arr[i] = None
	return arr


def _generate_column(rs: np.random.RandomState, name: str, rows: int) -> np.ndarray:
	# Minimal, plausible generation based solely on column names.
	# No schema semantics beyond names; distributions are intentionally simple and deterministic.
	lname = name.lower()
	if lname == "account_id":
		# Realistic: thousands of unique accounts
		n_accounts = min(5000, rows // 10)
		return rs.choice(np.arange(100000, 100000 + n_accounts), size=rows)
	if lname == "merchant_category":
		# Realistic: small set of categories
		categories = [
			"Retail", "Travel", "Food", "Grocery", "Utilities", "Health", "Entertainment", "Online", "Automotive", "Education"
		]
		return rs.choice(categories, size=rows)
	if lname in ("transaction_id", "id") or (lname.endswith("_id") and lname != "account_id"):
		# Deterministic sequential IDs starting from 1
		return np.arange(1, rows + 1, dtype=np.int64)
	if lname in ("amount", "txn_amount"):
		# Positive amounts, uniform in [1, 500], rounded to 2 decimals
		vals = rs.uniform(1.0, 500.0, size=rows)
		return np.round(vals, 2)
	if lname in ("is_fraud", "fraud"):
		# Placeholder; actual assignment handled separately with fraud_rate
		return np.zeros(rows, dtype=np.int8)
	if lname in ("timestamp", "datetime", "event_time", "date"):
		# Deterministic timestamps: fixed epoch, not version-based (see reviewer note)
		# We avoid timezone conversions to keep bytes stable when serialized.
		# Represent as ISO strings for stable serialization.
		base = 1735689600  # 2025-01-01T00:00:00Z as fixed fallback epoch
		# Spread by minutes
		return np.array([f"2025-01-01T00:{i%60:02d}:00Z" for i in range(rows)], dtype=object)
	# Generic string values for unknown columns
	return np.array([f"val_{i}" for i in range(rows)], dtype=object)


def _write_dataframe(df: pd.DataFrame, run_dir: str) -> None:
	# Write to parquet if available, else csv. Avoid partial files by using a temp file then rename.
	parquet_path = os.path.join(run_dir, "data.parquet")
	csv_path = os.path.join(run_dir, "data.csv")
	try:
		tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=run_dir)
		os.close(tmp_fd)
		try:
			df.to_parquet(tmp_path, index=False)
		except Exception as e:
			# Clean up temp and fall back to CSV
			try:
				os.remove(tmp_path)
			except Exception:
				pass
			raise e
		os.replace(tmp_path, parquet_path)
		return
	except Exception:
		# Fallback to CSV
		tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", dir=run_dir)
		os.close(tmp_fd)
		try:
			df.to_csv(tmp_path, index=False)
		except Exception as e:
			try:
				os.remove(tmp_path)
			except Exception:
				pass
			raise e
		os.replace(tmp_path, csv_path)


def generate_dataset(dataset_dir: str, configs: Dict[str, Any], run_dir: str) -> None:
	"""
	Deterministic synthetic data generation driven solely by configs.

	Inputs:
	  - dataset_dir: path to datasets/<dataset>/ (read-only)
	  - configs: dict of loaded YAMLs (dataset.yaml, schema.yaml, evolution.yaml)
	  - run_dir: path to runs/<dataset>/<version>/ where output must be written

	Output:
	  - Exactly one file written inside run_dir: data.parquet (preferred) or data.csv fallback

	Determinism:
	  - Random seed derived from dataset name and version (from run_dir), ensuring identical bytes for identical inputs.

	Failure semantics:
	  - Raises on missing required config, read/write failures; never returns partial results; never logs to stdout.
	"""
	_require_configs(configs)
	columns = _extract_columns(configs["schema.yaml"])
	rows = _extract_row_count(configs["dataset.yaml"])
	evo = _extract_evolution(configs["evolution.yaml"])

	seed = _derive_seed(dataset_dir, run_dir)

	data: Dict[str, np.ndarray] = {}
	for name in columns:
		# Per-column RNG to keep independent deterministic streams (deterministic hash)
		col_hash = _deterministic_col_hash(name)
		rs_col = np.random.RandomState((seed + col_hash) % (2 ** 32))
		col_vals = _generate_column(rs_col, name, rows)

		# Apply fraud rate if applicable
		if name.lower() in ("is_fraud", "fraud"):
			rate = evo.get("fraud_rate")
			if rate is None:
				raise KeyError("evolution.yaml must define fraud rate when 'is_fraud' column is present")
			k = int(round(rate * rows))
			idx = rs_col.permutation(rows)[:k]
			fraud = np.zeros(rows, dtype=np.int8)
			fraud[idx] = 1
			col_vals = fraud

		# Apply missingness deterministically per column if configured (config-driven only)
		miss_cfg = evo["missingness"].get(name)
		if miss_cfg is not None:
			col_vals = _apply_missingness(rs_col, col_vals, float(miss_cfg))

		data[name] = col_vals

	# Construct DataFrame with column order exactly matching schema.yaml
	df = pd.DataFrame({name: data[name] for name in columns})

	# Write output
	_write_dataframe(df, run_dir)



