# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------
INITIAL = {'regions': [{'region_id': 'R1', 'region_name': 'North', 'event_id': 'regions-initial-0', 'sequence': '1', 'updated_at': '2026-09-01T10:00:00Z', 'op': 'U'}], 'customers': [{'customer_id': 'C1', 'customer_name': 'Asha Singh', 'email': 'ASHA@example.com', 'region_id': 'R1', 'event_id': 'customers-initial-0', 'sequence': '1', 'updated_at': '2026-09-01T10:00:00Z', 'op': 'U'}], 'products': [{'product_id': 'P1', 'product_name': 'Notebook pack', 'category': 'Stationery', 'event_id': 'products-initial-0', 'sequence': '1', 'updated_at': '2026-09-01T10:00:00Z', 'op': 'U'}, {'product_id': 'P2', 'product_name': 'Pen set', 'category': 'Stationery', 'event_id': 'products-initial-1', 'sequence': '1', 'updated_at': '2026-09-01T10:00:00Z', 'op': 'U'}], 'orders': [{'order_id': 'O1', 'customer_id': 'C1', 'product_id': 'P1', 'order_date': '2026-09-01', 'quantity': '2', 'unit_price': '100.00', 'event_id': 'orders-initial-0', 'sequence': '1', 'updated_at': '2026-09-01T10:00:00Z', 'op': 'U'}, {'order_id': 'O2', 'customer_id': 'C1', 'product_id': 'P2', 'order_date': '2026-09-01', 'quantity': '1', 'unit_price': '50.00', 'event_id': 'orders-initial-1', 'sequence': '1', 'updated_at': '2026-09-01T10:00:00Z', 'op': 'U'}]}
INCREMENTAL = {'regions': [], 'customers': [{'customer_id': 'C1', 'customer_name': 'Asha Sharma', 'email': 'asha.sharma@example.com', 'region_id': 'R1', 'event_id': 'customer-update', 'sequence': '2', 'updated_at': '2026-09-02T10:00:00Z', 'op': 'U'}], 'products': [], 'orders': [{'order_id': 'O1', 'customer_id': 'C1', 'product_id': 'P1', 'order_date': '2026-09-01', 'quantity': '3', 'unit_price': '100.00', 'event_id': 'order-update', 'sequence': '2', 'updated_at': '2026-09-02T10:00:00Z', 'op': 'U'}, {'order_id': 'O2', 'sequence': '2', 'event_id': 'order-delete', 'updated_at': '2026-09-02T11:00:00Z', 'op': 'D'}, {'order_id': 'O3', 'customer_id': 'C1', 'product_id': 'P2', 'order_date': '2026-09-01', 'quantity': '2', 'unit_price': '50.00', 'event_id': 'order-new', 'sequence': '1', 'updated_at': '2026-09-02T12:00:00Z', 'op': 'U'}, {'order_id': 'BAD1', 'customer_id': 'C1', 'product_id': 'P1', 'order_date': '2026-09-01', 'quantity': '-2', 'unit_price': '100.00', 'event_id': 'order-invalid', 'sequence': '1', 'updated_at': '2026-09-01T10:00:00Z', 'op': 'U'}, {'order_id': 'O1', 'customer_id': 'C1', 'product_id': 'P1', 'order_date': '2026-09-01', 'quantity': '3', 'unit_price': '100.00', 'event_id': 'order-update', 'sequence': '2', 'updated_at': '2026-09-02T10:00:00Z', 'op': 'U'}, {'order_id': 'O1', 'customer_id': 'C1', 'product_id': 'P1', 'order_date': '2026-09-01', 'quantity': '2', 'unit_price': '100.00', 'event_id': 'orders-initial-0', 'sequence': '1', 'updated_at': '2026-09-01T10:00:00Z', 'op': 'U'}]}

import json
batch = dbutils.widgets.get('batch')
assert batch in ['initial','incremental']
for entity, rows in (INITIAL if batch == 'initial' else INCREMENTAL).items():
    if not rows:
        continue
    path = f'{ROOT}/landing/{entity}/{batch}.json'
    content = '\n'.join(json.dumps(x) for x in rows)+'\n'
    existing = {x.name for x in dbutils.fs.ls(f'{ROOT}/landing/{entity}')}
    if f'{batch}.json' not in existing:
        dbutils.fs.put(path, content, overwrite=False)
print('Immutable demo files ready. Seed is intentionally outside the recurring job.')
