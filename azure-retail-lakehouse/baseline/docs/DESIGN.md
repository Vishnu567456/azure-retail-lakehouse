# Data contracts and implementation choices

## Source envelope

Every JSON line is one entity change. Identifiers and input values are strings so invalid numeric/date fields can reach validation instead of causing ANSI conversion failures.

| Field | Contract |
|---|---|
| Entity business key | Nonempty string; unique current record per entity |
| event_id | Nonempty globally unique ID within an entity; retries preserve it |
| sequence | Positive integer, strictly increasing per business key |
| updated_at | Valid UTC-compatible timestamp for audit, not ordering |
| op | U for insert/update, D for deletion |
| Other fields | Full after-image on U; optional/null on D |

A source must never reuse a sequence for different content. Exact duplicated rows collapse. Conflicting same-sequence or reused-event-ID records fail the Silver task rather than choosing an arbitrary winner. A corrected rejected event must use a new event ID and higher sequence. Sequence is authoritative even if timestamps arrive out of order.

Malformed/rescued input, invalid envelopes, and failed payload checks go to an entity-specific quarantine snapshot. Unknown columns are deliberately treated as a contract change. Quarantine stores source file and ingestion time. It is a current recomputed rejection set, not an immutable incident ledger. Invalid newer events do not supersede the last valid state; monitor quarantine before trusting business freshness.

## Silver and deletion

The latest valid event per business key is selected with a descending sequence window. MERGE updates only where the incoming sequence is greater. Deletes remain as `is_deleted=true` rows, including when the key was not previously present. This preserves version ordering and prevents old updates from resurrecting deleted data. A later explicit U with a higher sequence can reactivate a key.

The code checks the complete valid event history before merging. This protects against source ambiguity but costs an increasing scan as history grows. No event-time watermark drops old events.

## Gold model

| Table | Grain |
|---|---|
| dim_customers | One active customer with current name, email, region |
| dim_products | One active product with current category |
| fact_orders | One active single-product order |
| daily_sales | One order date, summed order count, units, INR revenue |

Revenue = order quantity × order unit price using decimal arithmetic. It is gross merchandise value in this sample; tax, discounts, refunds, payment settlement, and multi-currency conversion are not modeled. Delete events remove an order from analytics; they do not represent a financial refund transaction.

Gold checks missing dimensions before writing any table. Unresolved foreign keys fail the task so orders are not silently dropped by inner joins. Deleted dimensions still referenced by active orders also cause failure. Provide the correct dimension version or fix the source transaction and rerun.

## Scaling path

For larger data, process new Bronze changes using Delta Change Data Feed or a checkpointed stream, retain an event/version conflict ledger, and perform idempotent foreachBatch merges with one writer per target. Benchmark these changes rather than assuming the current full scans scale. Incremental Gold must handle both old and new dates affected by corrections/deletes. Use an SCD2 dimension only when historical attribute analysis is required. Add versioned Gold publication if concurrent readers need a consistent snapshot across all tables.

Do not partition this tiny demo into many date folders. Add OPTIMIZE or clustering only when table size and query evidence justify it. Retain checkpoints and source files according to a documented recovery horizon; avoid short VACUUM retention.
