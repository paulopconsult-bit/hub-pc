# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based **data lakehouse ETL pipeline** following the Medallion Architecture (RAW → Bronze → Silver → Gold). It processes CSV client data through successive transformation layers, running either locally on Windows or in Google Cloud Storage (GCS) via Docker.

## Running the Pipeline

**Activate the virtual environment first:**
```powershell
.venv\Scripts\Activate.ps1
```

**Install dependencies:**
```powershell
pip install -r 00-scripts\00-cliente\requirements.txt
```

**Run the full pipeline locally (all 4 steps in sequence):**
```powershell
python 00-scripts\00-cliente\main-orchestrator-cliente.py
```

**Run a single pipeline step:**
```powershell
python 00-scripts\00-cliente\01-raw-02-bronze-cliente.py
python 00-scripts\00-cliente\02-bronze-03-silver-cleaned-cliente.py
python 00-scripts\00-cliente\03-silver-clean-04-silver-enriched-cliente.py
python 00-scripts\00-cliente\04-silver-enriched-05-gold-cliente.py
```

**Run via Docker (lab mode — writes logs to `88-logs-execution\`):**
```powershell
& D:\cloud\ls\00-scripts\00-cliente\run-lab-container-cliente.ps1
```
The PowerShell script builds the Docker image `hub-pc/microsvc-cliente:latest` on first run and reuses it thereafter. It mounts `D:\cloud\ls` to `/app/ls` inside the container.

**Validate Gold layer against cloud GCS:**
```powershell
python 100-staging-tests\00-cliente\04-05-test-parquet-gold-cloud.py
```
Requires `gcloud auth application-default login` to be run first.

## Architecture

### Execution Modes

Every script reads `EXECUTION_MODE` at startup (`local` by default). All I/O functions branch on this value — local mode uses Windows filesystem paths, cloud mode uses `gs://` URIs with the GCS SDK. This is the **S.A.L. (Storage Abstraction Layer)** pattern: the pipeline logic is identical regardless of environment.

### Data Layer Taxonomy

| Directory | Layer | Content |
|---|---|---|
| `01-00-raw/` | RAW | Input CSV drop zone |
| `02-00-raw-backup/` | Backup | CSVs moved here after successful ingestion |
| `02-01-bronze/` | Bronze | Parquet + control metadata columns added |
| `02-02-quarantine-erros/` | Quarantine | CSVs that fail schema contract validation |
| `03-silver-cleaned/` | Silver Cleaned | Cleaned Parquet (whitespace, chars, case) |
| `04-silver-enriched/approved/` | Silver Enriched | Typed, deduped, enriched records |
| `04-silver-enriched/rejected/` | Dead Letter | Records with invalid business data (e.g., impossible dates) |
| `05-gold-business/` | Gold | Final analytical product (`tb_cliente.parquet`) |
| `77-schema-controls/` | Schema-as-Code | JSON schema contracts — **tracked in git** |
| `88-logs-execution/` | Logs | Docker execution logs |
| `99-metadata-manifests/` | Manifests | Per-step JSON audit manifests |

### Pipeline Steps

1. **`01` RAW → Bronze**: Validates CSV columns against `77-schema-controls/<tenant>/schema-controls.json`, adds control fields (`_id_registro`, `_at_captura`, `_arquivo_origem`, `_pipeline_run_id`), writes Parquet, moves CSV to backup. Files failing schema validation go to quarantine.

2. **`02` Bronze → Silver Cleaned**: Reads all Parquet from Bronze, strips control characters and whitespace, normalizes `estado` to uppercase and `nome` to title case, writes a single `cliente.parquet`.

3. **`03` Silver Cleaned → Silver Enriched**: Deduplicates on `id_cliente` (keeping latest `_at_captura`), applies ID masking (`c00001` format), renames columns to snake_case (`nm_cliente`, `uf`, `dt_nascimento`), parses dates with `errors='coerce'`, calculates age. Records with invalid dates go to `rejected/`, valid ones to `approved/tb_cliente.parquet`.

4. **`04` Silver Enriched → Gold**: Adds `macro_regiao` business mapping (state → region), sorts by `id_cliente`, writes final `tb_cliente.parquet`. In cloud mode, Gold writes to a different bucket (`BUCKET_DW`) than the Lakehouse.

### Orchestrator & PIPELINE_RUN_ID

`main-orchestrator-cliente.py` generates a single `PIPELINE_RUN_ID` (e.g., `run-20260516-143022`) and injects it into all child processes via environment variable. This ensures every manifest, Bronze record, and audit field from a single run shares the same ID for full cross-layer traceability.

### Key Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `EXECUTION_MODE` | `local` | `local` or `cloud` |
| `TENANT_ID` | `00-cliente` | Scopes all data paths |
| `PIPELINE_RUN_ID` | auto-generated | Set by orchestrator; do not set manually |
| `ROOT_PROJETO` | `D:\cloud\ls` | Base path; Docker sets it to `/app/ls` |
| `BUCKET_LAKEHOUSE` | `stg-tf-hub-pc-dev` | GCS bucket for RAW → Silver Enriched |
| `BUCKET_DW` | `stg-dw-hub-pc-dev` | GCS bucket for Gold output |

### Schema Contract

`77-schema-controls/<tenant>/schema-controls.json` defines the expected CSV column order. Column order must match exactly — the Bronze step compares `list(df.columns)` against `colunas_esperadas`. This directory is the only JSON directory tracked in git (all other `*.json` files are gitignored).

### Manifest Protocol

Every pipeline step writes a JSON manifest with a fixed 11-key contract: `pipeline_run_id`, `tenant_id`, `camada_alvo`, `status_execucao`, `data_processamento`, `linhas_lidas`, `linhas_gravadas`, `linhas_rejeitadas`, `origem_caminho`, `destino_caminho`, `erro_detalhe`. Manifests are named `<layer>-<PIPELINE_RUN_ID>-<tenant>.json`.

### Partitioning

Files exceeding 1 GB are automatically partitioned by `_part_ano / _part_mes / _part_dia` using PyArrow partitioned Parquet. Below that threshold, a single flat Parquet file is written.

## Adding a New Domain (Tenant)

1. Create `00-scripts/<new-tenant>/` with the 5 scripts following the same naming convention.
2. Add `77-schema-controls/<new-tenant>/schema-controls.json` with the column contract.
3. Set `TENANT_ID=<new-tenant>` when running.
4. Create a new run script `run-lab-container-<new-tenant>.ps1` and Dockerfile.
