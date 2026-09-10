# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

for entity, (_, ddl) in ENTITIES.items():
    raw = (spark.readStream.format('cloudFiles')
      .option('cloudFiles.format','json')
      .option('cloudFiles.schemaLocation',f'{ROOT}/schemas/{entity}')
      .option('cloudFiles.schemaEvolutionMode','rescue')
      .option('rescuedDataColumn','_rescued_data')
      .schema(ddl + META)
      .load(f'{ROOT}/landing/{entity}')
      .withColumn('source_file',F.col('_metadata.file_path'))
      .withColumn('ingested_at',F.current_timestamp()))
    query = (raw.writeStream.format('delta').outputMode('append')
      .option('checkpointLocation',f'{ROOT}/checkpoints/bronze/{entity}')
      .trigger(availableNow=True).toTable(table('bronze',entity)))
    query.awaitTermination()
    audit(entity,'bronze_total',spark.table(table('bronze',entity)).count())
