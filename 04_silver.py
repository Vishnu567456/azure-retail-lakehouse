# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

# Small-project tradeoff: rescan Bronze history, but MERGE only newer versions.
# This deliberately avoids a fragile watermark / missed late-event design.
for entity, (key, _) in ENTITIES.items():
    df = (spark.table(table('bronze',entity))
      .withColumn('sequence',F.expr('try_cast(sequence as BIGINT)'))
      .withColumn('updated_at',F.expr('try_cast(updated_at as TIMESTAMP)'))
      .withColumn('op',F.upper(F.trim('op'))))
    valid = (F.col(key).isNotNull() & (F.length(F.trim(F.col(key))) > 0)
        & F.col('event_id').isNotNull() & (F.length(F.trim('event_id')) > 0)
        & F.col('sequence').isNotNull() & (F.col('sequence') > 0)
        & F.col('updated_at').isNotNull() & F.col('op').isin('U','D')
        & F.col('_rescued_data').isNull())
    if entity == 'orders':
        df = (df.withColumn('quantity',F.expr('try_cast(quantity as INT)'))
                .withColumn('unit_price',F.expr('try_cast(unit_price as DECIMAL(12,2))'))
                .withColumn('order_date',F.expr('try_cast(order_date as DATE)')))
        payload_ok = ((F.col('quantity') > 0) & (F.col('unit_price') >= 0)
            & F.col('order_date').isNotNull() & F.col('customer_id').isNotNull()
            & F.col('product_id').isNotNull())
    elif entity == 'customers':
        df = df.withColumn('email',F.lower(F.trim('email')))
        payload_ok = (F.col('customer_name').isNotNull() & F.col('region_id').isNotNull()
            & F.col('email').rlike(r'^[^@\s]+@[^@\s]+\.[^@\s]+$'))
    else:
        payload_ok = F.col('region_name' if entity == 'regions' else 'product_name').isNotNull()
    valid = F.coalesce(valid & ((F.col('op') == 'D') | payload_ok),F.lit(False))
    # Current rejection snapshot, not append-only error history.
    (df.filter(~valid).withColumn('reject_reason',F.lit('Invalid envelope, payload, or rescued fields'))
       .write.format('delta').mode('overwrite').option('overwriteSchema','true')
       .saveAsTable(table('ops',f'quarantine_{entity}')))
    clean = df.filter(valid).drop('_rescued_data','source_file','ingested_at')
    # Identical deliveries collapse; conflicting same-version payloads fail the run.
    clean = clean.dropDuplicates()
    if clean.groupBy('event_id').count().filter('count > 1').limit(1).count():
        raise ValueError(f'{entity}: event_id reused with different content')
    if clean.groupBy(key,'sequence').count().filter('count > 1').limit(1).count():
        raise ValueError(f'{entity}: conflicting versions for the same key and sequence')
    latest = (clean.withColumn('_rn',F.row_number().over(Window.partitionBy(key).orderBy(F.col('sequence').desc())))
                   .filter('_rn = 1').drop('_rn').withColumn('is_deleted',F.col('op') == 'D'))
    target = table('silver',entity)
    if not spark.catalog.tableExists(target):
        latest.limit(0).write.format('delta').saveAsTable(target)
    (DeltaTable.forName(spark,target).alias('t').merge(latest.alias('s'),f't.{key} = s.{key}')
      .whenMatchedUpdateAll(condition='s.sequence > t.sequence')
      .whenNotMatchedInsertAll().execute())
    # Tombstones are retained so late older updates cannot resurrect deleted keys.
    audit(entity,'silver_total',spark.table(target).count())
