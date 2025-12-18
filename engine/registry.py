import json
import os
from pathlib import Path
from typing import Dict, Any


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def update_registry_from_final_metadata(final_metadata_path: str, registry_path: str) -> None:
    """Update dataset registry from a finalized run.

    - Reads final_metadata.json
    - Appends a new version entry to registry/datasets.json
    - Updates latest_version
    - Deterministic ordering via sorted keys on write
    - Append-only semantics (no deletion or reordering of existing versions)
    - No filesystem scanning (only operates on provided paths)
    """
    final_meta_p = Path(final_metadata_path)
    reg_p = Path(registry_path)

    if not final_meta_p.is_file():
        raise FileNotFoundError(f"final_metadata.json not found: {final_meta_p}")
    if not reg_p.is_file():
        raise FileNotFoundError(f"registry file not found: {reg_p}")

    final_meta = _load_json(final_meta_p)

    dataset = final_meta.get("dataset")
    run_dir = final_meta.get("run_dir")
    finalized_at = final_meta.get("finalized_at_utc")

    if not dataset or not run_dir or not finalized_at:
        raise RuntimeError("final_metadata.json missing required fields (dataset, run_dir, finalized_at_utc)")

    # Derive immutable version identifier from run_dir name
    version = os.path.basename(os.path.normpath(run_dir))
    if not version:
        raise RuntimeError("Could not derive version from run_dir in final_metadata.json")

    registry = _load_json(reg_p)
    datasets = registry.get("datasets")
    if not isinstance(datasets, dict):
        raise RuntimeError("registry/datasets.json must contain a 'datasets' object")

    if dataset not in datasets:
        raise RuntimeError(f"Dataset '{dataset}' not found in registry; refusing to create implicit entry")

    entry = datasets[dataset]
    versions = entry.get("versions")
    if versions is None:
        versions = []
        entry["versions"] = versions
    if not isinstance(versions, list):
        raise RuntimeError("'versions' must be a list in registry entry")

    # Enforce append-only and no duplicate versions
    for v in versions:
        if v.get("version") == version:
            raise RuntimeError(f"Version '{version}' already present in registry for dataset '{dataset}'")

    versions.append({
        "version": version,
        "run_dir": run_dir,
        "finalized_at_utc": finalized_at,
    })
    entry["latest_version"] = version

    _atomic_write_json(reg_p, registry)
