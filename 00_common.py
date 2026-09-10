# Databricks notebook source
import re
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
for name, default in [('catalog','workspace'), ('prefix','vishnu_retail'), ('batch','initial'), ('run_id','manual')]:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
CAT = dbutils.widgets.get('catalog')
PREFIX = dbutils.widgets.get('prefix')
assert all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', x) for x in [CAT,PREFIX]), 'Use simple SQL identifiers'
spark.conf.set('spark.sql.session.timeZone','UTC')
def table(layer, name):
    return f'{CAT}.{PREFIX}_{layer}.{name}'
ROOT = f'/Volumes/{CAT}/{PREFIX}_ops/files'
ENTITIES = {
 'regions': ('region_id', 'region_id STRING, region_name STRING'),
 'customers': ('customer_id', 'customer_id STRING, customer_name STRING, email STRING, region_id STRING'),
 'products': ('product_id', 'product_id STRING, product_name STRING, category STRING'),
 'orders': ('order_id', 'order_id STRING, customer_id STRING, product_id STRING, order_date STRING, quantity STRING, unit_price STRING')
}
META = ', event_id STRING, sequence STRING, updated_at STRING, op STRING, _rescued_data STRING'
def audit(entity, stage, count):
    from datetime import datetime
    spark.createDataFrame([(dbutils.widgets.get('run_id'),entity,stage,int(count),datetime.utcnow())],
       'run_id string, entity string, stage string, row_count long, recorded_at timestamp').write.mode('append').saveAsTable(table('ops','audit'))
