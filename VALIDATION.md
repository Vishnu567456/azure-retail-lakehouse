# Validation record

Evidence: user-provided Azure Databricks screenshots from September 10, 2026. The successful advanced run lasted approximately 15 minutes 45 seconds. Account URLs, job IDs and screenshots containing account details are not included in this public repository.

The final notebook cell ran final_checks(), replayed ingestion/customer/web processing with no new files, asserted unchanged counts for customer_events, customer_profile_history, web_events, web_activity_5m and cdf_batches, then ran final_checks() again. All checks passed. This validates the supplied synthetic demo scenarios and this replay, not all possible operational failures.

Observed C1 history: North [Sep 1, Sep 2), West [Sep 2, Sep 3), South [Sep 3, open). Contact-only sequence 4 did not create a new region-history version. C2 has historical East plus a current deleted state. Finalized windows: 10:00 /shop=3; 10:05 /shop=1; 10:25 /checkout=2. CDF source counts: 2,2,3,1; last batch rejected one invalid record. Cross-batch event deduplication means per-batch valid counts must not be interpreted as globally unique events.

Two defects were found and fixed: int(None) when streaming metrics were unavailable; JSON serialization of UUID progress fields. Offline regression coverage includes both cases. Missing metrics remain null rather than claiming zero.

The original baseline initial load passed (two active orders, revenue 250). Its separate incremental fixture and alternative bundle are supplied but not claimed as verified in this record.

Screenshots show the deployed implementation's successful run. This repository removes deployment-specific identifiers and updates configuration for portability; it has not itself been deployed as a new Azure run.
