# Production-grade orchestration spine for synthetic data platform
import argparse
import sys
import os
import yaml
import json
from datetime import datetime, timezone

from engine.logging_utils import get_logger
from engine.registry import update_registry_from_final_metadata

# --- Project Root Resolution ---
def get_project_root():
	# Allow override for testing
	env_root = os.environ.get("SYNTH_DATA_PROJECT_ROOT")
	if env_root:
		return os.path.abspath(env_root)
	return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# --- CLI Parsing ---
def parse_args():
	parser = argparse.ArgumentParser(description="Synthetic Data Platform Orchestrator")
	parser.add_argument("--dataset", required=True, help="Dataset name (must match a directory in datasets/)")
	parser.add_argument("--run-id", required=False, help="Version/run identifier (for reproducibility). If omitted, uses current UTC timestamp.")
	return parser.parse_args()


# --- Directory containment check ---
from pathlib import Path
def is_within_dir(base: str, target: str) -> bool:
	base_path = Path(base).resolve()
	target_path = Path(target).resolve()
	try:
		target_path.relative_to(base_path)
		return True
	except ValueError:
		return False

# --- Config & Path Resolution ---
def resolve_dataset_dir(project_root, dataset_name):
	base = os.path.join(project_root, "datasets", dataset_name)
	abs_base = os.path.abspath(base)
	if not os.path.isdir(abs_base):
		sys.exit(f"ERROR: Dataset directory not found: {abs_base}")
	if not is_within_dir(project_root, abs_base):
		sys.exit(f"ERROR: Dataset directory escapes project root: {abs_base}")
	return abs_base

def load_yaml(path):
	abs_path = os.path.abspath(path)
	project_root = get_project_root()
	if not os.path.isfile(abs_path):
		sys.exit(f"ERROR: Required config missing: {abs_path}")
	if not is_within_dir(project_root, abs_path):
		sys.exit(f"ERROR: Config path escapes project root: {abs_path}")
	with open(abs_path, "r", encoding="utf-8") as f:
		return yaml.safe_load(f)

def ensure_dir(path):
	os.makedirs(path, exist_ok=True)

def snapshot_run_context(run_dir, configs, metadata):
	ensure_dir(run_dir)
	with open(os.path.join(run_dir, "configs_snapshot.json"), "w", encoding="utf-8") as f:
		json.dump(configs, f, indent=2)
	with open(os.path.join(run_dir, "run_metadata.json"), "w", encoding="utf-8") as f:
		json.dump(metadata, f, indent=2)

# --- Main Orchestration ---
def main():
	args = parse_args()
	project_root = get_project_root()
	dataset = args.dataset
	# Versioning: version is always the run-id (or timestamp if not provided)
	version = args.run_id or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

	dataset_dir = resolve_dataset_dir(project_root, dataset)
	config_files = ["dataset.yaml", "schema.yaml", "evolution.yaml"]
	configs = {}
	for fname in config_files:
		fpath = os.path.join(dataset_dir, fname)
		# Existence check only — semantic validation deferred
		configs[fname] = load_yaml(fpath)

	# Run context
	run_metadata = {
		"dataset": dataset,
		"version": version,
		"timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
		"project_root": project_root,
		"dataset_dir": dataset_dir,
		"run_dir": os.path.abspath(os.path.join(project_root, "runs", dataset, version)),
		"execution_order": [
			"config_loading",
			"version_resolution",
			"profiling",
			"generation",
			"validation",
			"evaluation",
			"artifact_persistence"
		],
		"engine_modules": [
			"profile_dataset",
			"generate_dataset",
			"validate_dataset",
			"evaluate_dataset",
			"persist_artifacts"
		]
	}

	logger = get_logger()
	logger.info(
		"pipeline_start",
		extra={"dataset": dataset, "version": version, "stage": "start"}
	)

	# Run directory: runs/<dataset>/<version>/
	run_dir = run_metadata["run_dir"]
	snapshot_run_context(run_dir, configs, run_metadata)

	# --- Orchestration Order ---
	logger.info(
		"stage_start",
		extra={"dataset": dataset, "version": version, "stage": "profiling", "run_dir": run_dir}
	)
	# 1. Profiling (if prior version exists)
	from engine.profile import profile_dataset
	runs_base = os.path.join(project_root, "runs", dataset)
	prior_versions = [
		v for v in os.listdir(runs_base)
		if os.path.isdir(os.path.join(runs_base, v)) and v < version
	] if os.path.isdir(runs_base) else []
	prior_versions_sorted = sorted(prior_versions, reverse=True)
	if prior_versions_sorted:
		prior_version = prior_versions_sorted[0]
		prior_version_dir = os.path.join(runs_base, prior_version)
		try:
			profile_dataset(dataset_dir, configs, run_dir, prior_version_dir)
		except Exception as e:
			logger.error(
				"stage_error",
				extra={
					"dataset": dataset,
					"version": version,
					"stage": "profiling",
					"run_dir": run_dir,
					"error": str(e),
				},
			)
			raise

	logger.info(
		"stage_start",
		extra={"dataset": dataset, "version": version, "stage": "generation", "run_dir": run_dir}
	)
	# 2. Generation
	from engine.generate import generate_dataset
	try:
		generate_dataset(dataset_dir, configs, run_dir)
	except Exception as e:
		logger.error(
			"stage_error",
			extra={
				"dataset": dataset,
				"version": version,
				"stage": "generation",
				"run_dir": run_dir,
				"error": str(e),
			},
		)
		raise

	logger.info(
		"stage_start",
		extra={"dataset": dataset, "version": version, "stage": "validation", "run_dir": run_dir}
	)
	# 3. Validation
	from engine.validate import validate_dataset
	try:
		validate_dataset(dataset_dir, configs, run_dir)
	except Exception as e:
		logger.error(
			"stage_error",
			extra={
				"dataset": dataset,
				"version": version,
				"stage": "validation",
				"run_dir": run_dir,
				"error": str(e),
			},
		)
		raise

	logger.info(
		"stage_start",
		extra={"dataset": dataset, "version": version, "stage": "evaluation", "run_dir": run_dir}
	)
	# 4. Evaluation
	from engine.evaluate import evaluate_dataset
	try:
		evaluate_dataset(dataset_dir, configs, run_dir)
	except Exception as e:
		logger.error(
			"stage_error",
			extra={
				"dataset": dataset,
				"version": version,
				"stage": "evaluation",
				"run_dir": run_dir,
				"error": str(e),
			},
		)
		raise

	logger.info(
		"stage_start",
		extra={"dataset": dataset, "version": version, "stage": "artifact_persistence", "run_dir": run_dir}
	)
	# 5. Artifact persistence
	from engine.artifacts import persist_artifacts
	try:
		persist_artifacts(dataset_dir, configs, run_dir)
	except Exception as e:
		logger.error(
			"stage_error",
			extra={
				"dataset": dataset,
				"version": version,
				"stage": "artifact_persistence",
				"run_dir": run_dir,
				"error": str(e),
			},
		)
		raise

	# Registry update (append-only, no filesystem scanning)
	logger.info(
		"stage_start",
		extra={"dataset": dataset, "version": version, "stage": "registry_update", "run_dir": run_dir}
	)
	try:
		final_meta_path = os.path.join(run_dir, "final_metadata.json")
		registry_path = os.path.join(project_root, "registry", "datasets.json")
		update_registry_from_final_metadata(final_meta_path, registry_path)
	except Exception as e:
		logger.error(
			"stage_error",
			extra={
				"dataset": dataset,
				"version": version,
				"stage": "registry_update",
				"run_dir": run_dir,
				"error": str(e),
			},
		)
		raise

	logger.info(
		"pipeline_end",
		extra={"dataset": dataset, "version": version, "stage": "end", "run_dir": run_dir}
	)

if __name__ == "__main__":
	main()
