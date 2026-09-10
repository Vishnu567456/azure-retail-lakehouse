# Advanced retail demo

Run baseline setup and initial load first. The extension reuses the four vishnu_retail schemas and the files volume, under the catalog you select. It creates advanced_v1-prefixed tables and isolated landing/checkpoint directories.

## Configure and deploy from Azure Cloud Shell

```bash
export DATABRICKS_HOST="https://YOUR-WORKSPACE-HOST"
export RETAIL_CATALOG="YOUR_CATALOG"
python3 advanced/deploy.py
```

Default deployment creates/imports the job and notebooks without submitting compute. Set the catalog widget for manual notebook use; the deployment script passes RETAIL_CATALOG for job execution. Existing modified notebooks are intentionally not overwritten. If the workspace already has this project, export it first and reconcile differences; Databricks may add environment metadata to exported source.

```bash
# Starts paid compute; run only when ready:
python3 advanced/deploy.py --run
# Read-only status:
python3 advanced/deploy.py --status YOUR_RUN_ID
```

The initial submission uses a stable idempotency token. Repeating it returns the same submission; it is not a new recovery attempt. After a failed run, inspect its traceback before repairing or submitting a deliberate recovery. Do not clear checkpoints to recover.

The demo processes five ordered fixture phases, validates SCD1/SCD2, late history, soft deletion, quarantine, CDF consumption, watermark behavior and window totals, then replays without new input and validates again. Checkpointed streams use AvailableNow. Manual execution only; no schedule. Job and task timeout: 1800 seconds. Standard serverless performance, one concurrent run, zero configured task retries.

Progress logging handles nullable metrics and UUID values. The obsolete one-off repair script and historical job IDs are excluded from this public package.

Offline tests: `python3 -m unittest discover -s advanced/tests -v` from repository root. These test fixtures and mocked interfaces, not a live Spark runtime. See ../docs/VALIDATION.md for observed Azure evidence.
