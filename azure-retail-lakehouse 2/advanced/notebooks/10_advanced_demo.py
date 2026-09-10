# Databricks notebook source
# MAGIC %run ./00_advanced_common

# COMMAND ----------
PHASES = [{'phase': 1, 'customers': [{'event_id': 'c1-v1', 'customer_id': 'C1', 'sequence': 1, 'effective_at': '2026-09-01T00:00:00Z', 'customer_name': 'Asha Sharma', 'email': 'asha@example.com', 'region': 'North', 'op': 'U'}, {'event_id': 'c2-v1', 'customer_id': 'C2', 'sequence': 1, 'effective_at': '2026-09-01T00:00:00Z', 'customer_name': 'Ravi Kumar', 'email': 'ravi@example.com', 'region': 'East', 'op': 'U'}], 'web': [{'event_id': 'e1', 'event_time': '2026-09-01T10:01:00Z', 'page': '/shop'}, {'event_id': 'e2', 'event_time': '2026-09-01T10:03:00Z', 'page': '/shop'}, {'event_id': 'e2', 'event_time': '2026-09-01T10:03:00Z', 'page': '/shop'}]}, {'phase': 2, 'customers': [{'event_id': 'c1-v3', 'customer_id': 'C1', 'sequence': 3, 'effective_at': '2026-09-03T00:00:00Z', 'customer_name': 'Asha Sharma', 'email': 'asha.new@example.com', 'region': 'South', 'op': 'U'}, {'event_id': 'c2-delete', 'customer_id': 'C2', 'sequence': 2, 'effective_at': '2026-09-02T00:00:00Z', 'customer_name': 'Ravi Kumar', 'email': None, 'region': None, 'op': 'D'}], 'web': [{'event_id': 'e3', 'event_time': '2026-09-01T10:08:00Z', 'page': '/shop'}, {'event_id': 'e4', 'event_time': '2026-09-01T10:02:00Z', 'page': '/shop'}, {'event_id': 'e2', 'event_time': '2026-09-01T10:03:00Z', 'page': '/shop'}]}, {'phase': 3, 'customers': [{'event_id': 'c1-v2-late', 'customer_id': 'C1', 'sequence': 2, 'effective_at': '2026-09-02T00:00:00Z', 'customer_name': 'Asha Sharma', 'email': 'asha.west@example.com', 'region': 'West', 'op': 'U'}, {'event_id': 'c1-v4', 'customer_id': 'C1', 'sequence': 4, 'effective_at': '2026-09-04T00:00:00Z', 'customer_name': 'Asha Sharma', 'email': 'asha.final@example.com', 'region': 'South', 'op': 'U'}, {'event_id': 'c1-v3', 'customer_id': 'C1', 'sequence': 3, 'effective_at': '2026-09-03T00:00:00Z', 'customer_name': 'Asha Sharma', 'email': 'asha.new@example.com', 'region': 'South', 'op': 'U'}], 'web': [{'event_id': 'e5', 'event_time': '2026-09-01T10:25:00Z', 'page': '/checkout'}]}, {'phase': 4, 'customers': [{'event_id': 'c3-invalid', 'customer_id': 'C3', 'sequence': 0, 'effective_at': '2026-09-04T00:00:00Z', 'customer_name': 'Ravi Kumar', 'email': 'invalid-email', 'region': 'North', 'op': 'U'}], 'web': [{'event_id': 'too-late', 'event_time': '2026-09-01T10:01:00Z', 'page': '/shop'}, {'event_id': 'e6', 'event_time': '2026-09-01T10:26:00Z', 'page': '/checkout'}]}, {'phase': 5, 'customers': [], 'web': [{'event_id': 'e7', 'event_time': '2026-09-01T10:45:00Z', 'page': '/home'}]}]

# COMMAND ----------
create_tables()
completed={r.phase for r in spark.table(name('ops','phase_status')).select('phase').collect()}
for phase in PHASES:
    number=phase['phase']
    if number in completed:
        print(f'Phase {number} already completed; preserving its temporal ordering')
        continue
    land(phase)
    ingest_bronze()
    process_customers()
    process_web(number)
    assert_customer_results(number)
    mark_phase(number)
    print(f'Phase {number} passed')

# COMMAND ----------
final_checks()
# Reprocess with no new source data: business outputs and CDF batch count must not change.
before={t:spark.table(t).count() for t in [name('silver','customer_events'),name('silver','customer_profile_history'),name('silver','web_events'),name('gold','web_activity_5m'),name('ops','cdf_batches')]}
ingest_bronze()
process_customers()
process_web(6)
after={t:spark.table(t).count() for t in before}
assert before==after, f'Replay changed output counts: {before} -> {after}'
final_checks()
print('PASS: replay with no new input produced unchanged results')

# COMMAND ----------
# Small business integration: compare baseline order revenue by current customer region.
# This illustrates current-region attribution, not an order-time SCD2 join.
facts=f'{CAT}.{PREFIX}_gold.fact_orders'
if spark.catalog.tableExists(facts):
    spark.sql(f"""CREATE OR REPLACE VIEW {name('gold','sales_by_current_region')} AS
      SELECT c.region, COUNT(*) AS order_count, SUM(f.revenue) AS revenue
      FROM {facts} f JOIN {name('silver','customer_contacts')} c
        ON f.customer_id=c.customer_id
      WHERE NOT c.is_deleted GROUP BY c.region""")
display(spark.table(name('silver','customer_contacts')))
display(spark.table(name('silver','customer_profile_history')).orderBy('customer_id','valid_from'))
display(spark.table(name('gold','web_activity_5m')).orderBy('window_start'))
display(spark.table(name('ops','cdf_batches')).orderBy('batch_id'))
