<div align="center">

# 🧪 Synthetic Data Platform

**Production-grade, deterministic synthetic data generation for ML pipelines**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![CI](https://github.com/username/synthetic-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/username/synthetic-data-platform/actions)

</div>

---

## Overview

A config-driven pipeline for generating, validating, and publishing synthetic datasets with **strict immutability**, **deterministic outputs**, and **full auditability**. Designed for ML teams that need reproducible training data without the compliance overhead of real data.

### Key Features

- **100% Config-Driven** — No hardcoded values; all behavior controlled via YAML
- **Deterministic Generation** — Same config + version = identical bytes every time
- **Immutable Runs** — Once finalized, runs cannot be modified or overwritten
- **Schema Enforcement** — Strict type/constraint validation against declared schemas
- **Drift Detection** — Automatic quality and distribution drift metrics vs prior versions
- **Kaggle Publishing** — One-command upload to Kaggle with versioned metadata
- **Structured Logging** — JSON logs for pipeline observability (optional)
- **CI-Ready** — GitHub Actions workflow included

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR                             │
│                      scripts/run.py                             │
│  CLI → Config Loading → Version Resolution → Stage Execution    │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
   │   PROFILE   │    │  GENERATE   │    │  VALIDATE   │
   │ Prior data  │    │ Synthetic   │    │  Schema +   │
   │  analysis   │    │   output    │    │ constraints │
   └─────────────┘    └─────────────┘    └─────────────┘
          │                   │                   │
          └───────────────────┼───────────────────┘
                              ▼
                    ┌─────────────────┐
                    │    EVALUATE     │
                    │ Quality + Drift │
                    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   ARTIFACTS     │
                    │  Finalization   │
                    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │    REGISTRY     │
                    │ Version catalog │
                    └─────────────────┘
```

---

## Quick Start

### Installation

```bash
git clone https://github.com/username/synthetic-data-platform.git
cd synthetic-data-platform

# Install dependencies
pip install -r requirements.txt

# (Optional) Install dev dependencies for testing
pip install -r requirements-dev.txt
```

### Generate Your First Dataset

```bash
python scripts/run.py --dataset finance_transactions
```

This will:
1. Load configs from `datasets/finance_transactions/`
2. Generate deterministic synthetic data
3. Validate against schema constraints
4. Compute quality metrics
5. Finalize artifacts in `runs/finance_transactions/<version>/`
6. Update the registry

### Reproducible Runs

```bash
# Fixed version for byte-identical reproduction
python scripts/run.py --dataset finance_transactions --run-id 2025-01-15T00-00-00Z
```

---

## Project Structure

```
synthetic-data-platform/
├── scripts/
│   └── run.py                 # CLI orchestrator
├── engine/
│   ├── profile.py             # Prior version profiling
│   ├── generate.py            # Deterministic data generation
│   ├── validate.py            # Schema/constraint validation
│   ├── evaluate.py            # Quality and drift metrics
│   ├── artifacts.py           # Immutability enforcement
│   ├── version.py             # Version identity resolution
│   ├── ingest.py              # External dataset ingestion
│   ├── kaggle.py              # Kaggle upload with retry
│   ├── registry.py            # Registry update logic
│   └── logging_utils.py       # JSON structured logging
├── datasets/
│   ├── finance_transactions/
│   │   ├── dataset.yaml       # Row count, metadata
│   │   ├── schema.yaml        # Column definitions
│   │   └── evolution.yaml     # Drift/missingness config
│   ├── market_time_series/
│   └── saas_events/
├── registry/
│   └── datasets.json          # Authoritative version catalog
├── runs/                       # Generated at runtime
│   └── <dataset>/<version>/
├── tests/
│   ├── test_version.py
│   ├── test_generate.py
│   ├── test_validate.py
│   ├── test_profile.py
│   ├── test_evaluate.py
│   ├── test_artifacts.py
│   └── test_integration.py
├── notebooks/
│   └── run_dataset.ipynb      # Interactive runner
├── .github/workflows/
│   └── ci.yml                 # GitHub Actions CI
├── requirements.txt           # Pinned runtime deps
├── requirements-dev.txt       # Dev/test deps
└── README.md
```

---

## Configuration Reference

### dataset.yaml

```yaml
name: finance_transactions
domain: finance
description: Synthetic transactional data for ML training
row_count: 10000
```

### schema.yaml

```yaml
columns:
  transaction_id:
    type: integer
    nullable: false
  amount:
    type: float
    nullable: false
    constraints:
      min: 0.01
      max: 10000.0
  is_fraud:
    type: boolean
    nullable: false
```

**Supported types:** `string`, `integer`, `float`, `boolean`, `datetime`

### evolution.yaml

```yaml
fraud_rate: 0.02          # 2% of rows marked as fraud
missingness:
  merchant_category: 0.05  # 5% nulls in this column
```

---

## Run Artifacts

Each run produces these files in `runs/<dataset>/<version>/`:

| File | Description |
|------|-------------|
| `data.parquet` | Generated dataset (Parquet preferred) |
| `data.csv` | Fallback if Parquet unavailable |
| `configs_snapshot.json` | Frozen copy of input configs |
| `run_metadata.json` | Execution context and timestamps |
| `validation_report.json` | Schema validation results |
| `evaluation_report.json` | Quality and drift metrics |
| `prior_profile.json` | Prior version statistics (if exists) |
| `final_metadata.json` | Finalization manifest (immutability marker) |

---

## API Reference

### Orchestrator CLI

```bash
python scripts/run.py --dataset <name> [--run-id <version>]
```

### Programmatic Usage

```python
# Generate
from engine.generate import generate_dataset
generate_dataset(dataset_dir, configs, run_dir)

# Validate
from engine.validate import validate_dataset
validate_dataset(dataset_dir, configs, run_dir)

# Ingest external data
from engine.ingest import ingest_external_dataset
ingest_external_dataset("external.parquet", "runs/imports/v1")

# Publish to Kaggle
from engine.kaggle import upload_to_kaggle
upload_to_kaggle("runs/finance_transactions/v1", "user/dataset-name")
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SDP_LOGGING_ENABLED` | `true` | Enable JSON logging |
| `SDP_LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |
| `KAGGLE_CONFIG_DIR` | `~/.kaggle` | Kaggle credentials location |

---

## Testing

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=engine --cov-report=term-missing

# Run specific test
pytest tests/test_generate.py -v
```

---

## CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push/PR:
- Python 3.11 on Ubuntu
- Installs pinned dependencies
- Runs pytest with fail-fast

---

## Kaggle Publishing

```bash
# Ensure credentials exist
ls ~/.kaggle/kaggle.json

# Upload finalized run
python -c "
from engine.kaggle import upload_to_kaggle
upload_to_kaggle(
    run_dir='runs/finance_transactions/2025-01-15T00-00-00Z',
    kaggle_slug='username/finance-synthetic',
    is_public=True
)
"
```

**Upload includes only:**
- `data.parquet` or `data.csv`
- `final_metadata.json`

---

## Determinism Guarantees

| Aspect | Guarantee |
|--------|-----------|
| Random seed | Derived from `SHA256(dataset_name:version)` |
| Column order | Lexicographically sorted |
| Row order | Preserved from generation/ingestion |
| Timestamps | UTC, ISO-8601 format |
| JSON output | Sorted keys, deterministic formatting |

---

## Known Limitations

1. **CSV dtype inference** — Pandas may infer different types across platforms; documented, not fixed by contract
2. **Local filesystem only** — No native S3/GCS/Azure Blob support
3. **Single-process** — No parallelization or distributed generation
4. **Basic generation heuristics** — Column values inferred from names only; no statistical modeling
5. **Kaggle single-retry** — One retry with 2s backoff; no exponential backoff

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests (`pytest tests/`)
4. Commit changes (`git commit -m 'Add amazing feature'`)
5. Push to branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

**Built for ML teams who need reproducible, compliant synthetic data.**

</div>
