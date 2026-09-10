# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

from decimal import Decimal
for entity,(key,_) in ENTITIES.items():
    assert not spark.table(table('silver',entity)).groupBy(key).count().filter('count > 1').limit(1).count()
fact = spark.table(table('gold','fact_orders'))
expected = Decimal('250.00') if dbutils.widgets.get('batch') == 'initial' else Decimal('400.00')
assert fact.count() == 2, 'Demo expects two active orders'
assert fact.agg(F.sum('revenue')).first()[0] == expected
if dbutils.widgets.get('batch') == 'incremental':
    assert spark.table(table('silver','orders')).filter("order_id = 'O2' AND is_deleted").count() == 1
    assert spark.table(table('ops','quarantine_orders')).filter("order_id = 'BAD1'").count() == 1
    assert spark.table(table('gold','dim_customers')).filter("customer_name = 'Asha Sharma'").count() == 1
print(f'Demo checks passed; revenue = {expected}. Rerun Bronze/Silver/Gold and this notebook to test idempotence.')
