import os
import json
import pandas as pd
import numpy as np

def validate_dataset(dataset_dir, configs, run_dir):
	"""
	Strictly validates generated data against schema.yaml and invariants.
	Inputs:
	  - dataset_dir: path to datasets/<dataset>/ (read-only)
	  - configs: loaded YAML configs (dataset.yaml, schema.yaml, evolution.yaml)
	  - run_dir: path to runs/<dataset>/<version>/ (read/write for outputs)
	Output:
	  - Writes exactly one file: validation_report.json in run_dir if validation passes
	Failure:
	  - Raises immediately on any violation; never writes partial reports
	"""
	# 1. Locate generated data
	data_path = None
	for fname in ["data.parquet", "data.csv"]:
		fpath = os.path.join(run_dir, fname)
		if os.path.isfile(fpath):
			data_path = fpath
			break
	if data_path is None:
		raise FileNotFoundError(f"No generated data file found in {run_dir} (searched for data.parquet, data.csv)")

	# 2. Load data
	try:
		if data_path.endswith(".parquet"):
			df = pd.read_parquet(data_path)
		elif data_path.endswith(".csv"):
			df = pd.read_csv(data_path)
		else:
			raise ValueError("Unsupported data file type for validation.")
	except Exception as e:
		raise RuntimeError(f"Failed to read generated data file: {data_path}") from e

	# 3. Parse schema
	schema = configs.get("schema.yaml")
	if not schema or "columns" not in schema:
		raise KeyError("schema.yaml must contain 'columns'")
	columns_cfg = schema["columns"]
	# Support mapping (preferred) or list fallback
	if isinstance(columns_cfg, dict):
		schema_cols = list(columns_cfg.keys())
		schema_defs = columns_cfg
	elif isinstance(columns_cfg, list):
		schema_cols = []
		schema_defs = {}
		for c in columns_cfg:
			if isinstance(c, str):
				schema_cols.append(c)
				schema_defs[c] = {}
			elif isinstance(c, dict) and "name" in c:
				schema_cols.append(c["name"])
				schema_defs[c["name"]] = c
			else:
				raise ValueError("Invalid column entry in schema.yaml; expected string or dict with 'name'")
	else:
		raise ValueError("schema.yaml 'columns' must be a mapping or a list")

	# 4. Strict schema enforcement
	# a. All schema columns must exist, no extras allowed
	data_cols = list(df.columns)
	if set(data_cols) != set(schema_cols):
		missing = sorted(set(schema_cols) - set(data_cols))
		extra = sorted(set(data_cols) - set(schema_cols))
		msg = []
		if missing:
			msg.append(f"Missing columns: {missing}")
		if extra:
			msg.append(f"Extra columns: {extra}")
		raise ValueError("Schema column mismatch: " + "; ".join(msg))

	# b. Data type compatibility and nullability
	# Always validate in sorted column order for determinism
	for col in sorted(schema_cols):
		col_def = schema_defs.get(col, {})
		# Type check: enforce schema vocabulary
		declared_type = col_def.get("type")
		if declared_type:
			dtype_map = {
				"string": ["object", "string"],
				"float": ["float64", "float32", "float16"],
				"integer": ["int64", "int32", "int16", "int8", "uint8", "uint16", "uint32", "uint64"],
				"boolean": ["bool", "boolean", "int8", "int32", "int64"],
				"datetime": ["datetime64[ns]", "datetime64[ns, UTC]", "object", "string"]
			}
			actual_dtype = str(df[col].dtype)
			valid_types = dtype_map.get(declared_type)
			if not valid_types:
				raise TypeError(f"Column '{col}' type '{declared_type}' is not a supported schema type")
			# Datetime: accept pandas datetime or ISO-8601 strings that can be parsed
			if declared_type == "datetime":
				if actual_dtype.startswith("datetime64"):
					pass  # OK
				else:
					# Try to parse as datetime
					try:
						pd.to_datetime(df[col], errors="raise")
					except Exception:
						raise TypeError(f"Column '{col}' type mismatch: expected datetime, got {actual_dtype} (not parseable)")
			else:
				if actual_dtype not in valid_types:
					raise TypeError(f"Column '{col}' type mismatch: expected {declared_type}, got {actual_dtype}")
		# Nullability
		nullable = col_def.get("nullable", True)
		if nullable is False:
			if df[col].isnull().any():
				raise ValueError(f"Column '{col}' is not nullable but contains nulls")
		# min/max constraints (numeric only, from constraints mapping)
		if declared_type in ("float", "integer"):
			constraints = col_def.get("constraints", {})
			minv = constraints.get("min")
			maxv = constraints.get("max")
			if minv is not None:
				if (df[col] < minv).any():
					raise ValueError(f"Column '{col}' violates min constraint: {minv}")
			if maxv is not None:
				if (df[col] > maxv).any():
					raise ValueError(f"Column '{col}' violates max constraint: {maxv}")

	# 5. Output report
	report = {
		"status": "pass",
		"row_count": int(df.shape[0]),
		"column_count": int(df.shape[1])
	}
	out_path = os.path.join(run_dir, "validation_report.json")
	try:
		with open(out_path, "w", encoding="utf-8") as f:
			json.dump(report, f, indent=2, sort_keys=False, ensure_ascii=False)
	except Exception as e:
		raise RuntimeError(f"Failed to write validation report: {out_path}") from e
