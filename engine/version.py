from datetime import datetime

def resolve_version(dataset_name, run_id=None):
    if not dataset_name or not isinstance(dataset_name, str):
        raise ValueError("dataset_name must be a non-empty string")
    if run_id is not None:
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty string if provided")
        return run_id
    # Deterministic UTC timestamp, zero-padded
    now = datetime.utcnow()
    return now.strftime("%Y-%m-%dT%H-%M-%SZ")