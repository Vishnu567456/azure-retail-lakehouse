# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

# Requires an existing catalog with managed storage and schema creation permission.
for layer in ['bronze','silver','gold','ops']:
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {CAT}.{PREFIX}_{layer}')
# An administrator may precreate this as an EXTERNAL volume; IF NOT EXISTS preserves it.
spark.sql(f'CREATE VOLUME IF NOT EXISTS {CAT}.{PREFIX}_ops.files')
for entity in ENTITIES:
    dbutils.fs.mkdirs(f'{ROOT}/landing/{entity}')
    ddl = ENTITIES[entity][1] + META + ', source_file STRING, ingested_at TIMESTAMP'
    spark.sql(f'CREATE TABLE IF NOT EXISTS {table("bronze",entity)} ({ddl}) USING DELTA')
print(f'Ready: {ROOT}')
