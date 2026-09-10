# Operations and recovery

| Symptom | Action |
|---|---|
| Catalog not found | Select your actual Unity Catalog catalog in every notebook/job parameter |
| Managed storage unavailable | Ask the catalog administrator to configure managed storage |
| Volume permission error | Check USE CATALOG, USE SCHEMA, READ VOLUME, WRITE VOLUME and compute compatibility |
| External location validation fails | Check connector identity, container-scoped RBAC, storage network rules and path |
| Bronze task fails | Inspect job error, correct configuration, retry with the same checkpoints |
| Silver conflict error | Inspect reused event IDs or same-key sequences; resolve at source and investigate affected Bronze data before rerun |
| Gold missing dimension error | Land missing/corrected dimension events, run Bronze → Silver → Gold again |
| New field appears | Inspect quarantine/rescued data; review and update explicit source/target schema before reprocessing |
| Revenue does not match demo | Check batch widget, unique prefix, namespace consistency and task completion |

Use immutable new filenames for new source files. Do not overwrite an ingested filename expecting Auto Loader to ingest it as a new event. Do not delete checkpoints during routine retries. If a checkpoint is irrecoverably lost, plan a full controlled rebuild in a new namespace from retained source files; compare results before switching consumers.

Conflicting Bronze history is intentionally a hard failure. A new event does not erase earlier ambiguity. Reconcile the source of truth and rebuild a clean namespace with audited corrected history rather than casually deleting historical rows.

Audit queries are in ANALYTICS.sql. Lakeflow Jobs task state and logs are the authoritative failure signal: the audit helper records successful stage totals, not every failure. Audit count is a total table snapshot, not the number processed in that run. A retry can append another audit entry. Configure a failure notification and inspect quarantine counts after every demonstration.

The job is limited to one concurrent run. Avoid manual writes while it is running. Tables are not protected from independent jobs sharing the same prefix; use separate prefixes or enforce a single writer operationally.

Gold writes are individually atomic but not a multi-table transaction. If Gold fails during publication, readers may see mixed generations; repair and rerun the whole Gold task, then require its successful completion before consuming results.

For teardown, stop compute and disable any schedules you added. Retain the project until you have exported needed data. Delete only the project's explicitly identified resources through your normal Azure/Databricks process; no destructive teardown script is included.
