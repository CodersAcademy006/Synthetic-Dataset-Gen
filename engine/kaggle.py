def upload_to_kaggle(run_dir, kaggle_slug, is_public=True):
    """Publish a finalized, immutable run as a Kaggle dataset version.

    Args:
        run_dir (str): Path to finalized run directory.
        kaggle_slug (str): Kaggle dataset identifier (username/dataset-name).
        is_public (bool): Whether the dataset is public (default: True).

    Raises:
        RuntimeError: On any contract violation or Kaggle API failure.
    """
    import os
    from pathlib import Path
    import json
    import time

    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError as e:
        raise RuntimeError("Kaggle API is not installed. Please install 'kaggle'.") from e

    run_dir = Path(run_dir)
    if not run_dir.exists() or not run_dir.is_dir():
        raise RuntimeError(f"run_dir does not exist or is not a directory: {run_dir}")

    # Basic slug validation (dry-run check, no network)
    if not isinstance(kaggle_slug, str) or "/" not in kaggle_slug or kaggle_slug.count("/") != 1:
        raise RuntimeError("kaggle_slug must be of the form 'username/dataset-name'")

    final_metadata_path = run_dir / "final_metadata.json"
    if not final_metadata_path.is_file():
        raise RuntimeError("final_metadata.json not found in run_dir; run must be finalized before upload")

    # Data file: exactly one of data.parquet or data.csv
    data_parquet = run_dir / "data.parquet"
    data_csv = run_dir / "data.csv"
    has_parquet = data_parquet.is_file()
    has_csv = data_csv.is_file()
    if has_parquet and has_csv:
        raise RuntimeError("Ambiguous data files in run_dir: both data.parquet and data.csv exist")
    if not has_parquet and not has_csv:
        raise RuntimeError("Missing data file in run_dir: neither data.parquet nor data.csv exist")
    data_file = data_parquet if has_parquet else data_csv

    # Kaggle credentials check (dry-run validation, no network yet)
    kaggle_config_dir = os.environ.get("KAGGLE_CONFIG_DIR")
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if kaggle_config_dir:
        config_path = Path(kaggle_config_dir) / "kaggle.json"
        if not config_path.is_file():
            raise RuntimeError("KAGGLE_CONFIG_DIR is set but kaggle.json not found")
    elif not kaggle_json.is_file():
        raise RuntimeError("Kaggle credentials not found in ~/.kaggle/kaggle.json or KAGGLE_CONFIG_DIR")

    # Load manifest for notes
    with final_metadata_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)
    dataset_name = manifest.get("dataset")
    finalized_at = manifest.get("finalized_at_utc")
    if not dataset_name or not finalized_at:
        raise RuntimeError("final_metadata.json missing required fields: 'dataset' and/or 'finalized_at_utc'")

    run_version = run_dir.name
    if not run_version:
        raise RuntimeError("Could not derive version from run_dir name")

    notes = (
        f"Dataset: {dataset_name}\n"
        f"Run version: {run_version}\n"
        f"Finalized at (UTC): {finalized_at}"
    )

    # Prepare upload files (allowed artifacts only)
    files_to_upload = [str(data_file), str(final_metadata_path)]

    # Prepare Kaggle dataset metadata file
    meta = {
        "id": kaggle_slug,
        "title": dataset_name,
        "licenses": [{"name": "CC0-1.0"}],
        "isPrivate": not is_public,
    }
    meta_path = run_dir / "dataset-metadata.json"
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    api = KaggleApi()
    try:
        api.authenticate()
    except Exception as e:
        raise RuntimeError("Failed to authenticate with Kaggle API; check your kaggle.json credentials.") from e

    # Determine whether dataset exists
    exists = False
    try:
        api.dataset_metadata(kaggle_slug)
        exists = True
    except Exception:
        exists = False

    # Build upload directory (only allowed artifacts)
    upload_dir = run_dir / "kaggle_upload"
    if upload_dir.exists():
        for f in upload_dir.iterdir():
            f.unlink()
        upload_dir.rmdir()
    upload_dir.mkdir()
    for f in files_to_upload + [str(meta_path)]:
        dest = upload_dir / Path(f).name
        with open(f, "rb") as src, open(dest, "wb") as dst:
            dst.write(src.read())

    def _do_upload_once() -> None:
        if exists:
            api.dataset_create_version(
                str(upload_dir),
                version_notes=notes,
            )
        else:
            api.dataset_create_new(
                str(upload_dir),
                public=is_public,
                dir_mode="zip",
                convert_to_csv=False,
            )

    upload_success = False
    try:
        try:
            _do_upload_once()
            upload_success = True
        except Exception as first_err:
            # Single retry with backoff, no silent failure
            time.sleep(2.0)
            try:
                _do_upload_once()
                upload_success = True
            except Exception as second_err:
                raise RuntimeError(
                    "Kaggle upload failed after one retry; last error: " f"{second_err}"
                ) from second_err
    finally:
        if upload_success:
            for f in upload_dir.iterdir():
                f.unlink()
            upload_dir.rmdir()
            meta_path.unlink()