# Databricks notebook source
import json
import re
from datetime import datetime
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
for name,default in [('catalog','workspace'),('prefix','vishnu_retail'),('demo_id','advanced_v1')]:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name,default)
CAT = dbutils.widgets.get('catalog')
PREFIX = dbutils.widgets.get('prefix')
DEMO = dbutils.widgets.get('demo_id')
assert all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',x) for x in (CAT,PREFIX,DEMO))
spark.conf.set('spark.sql.session.timeZone','UTC')
ROOT = f'/Volumes/{CAT}/{PREFIX}_ops/files/{DEMO}'
def name(layer,entity):
    return f'{CAT}.{PREFIX}_{layer}.{DEMO}_{entity}'
CUSTOMER_DDL = 'event_id STRING, customer_id STRING, sequence BIGINT, effective_at STRING, customer_name STRING, email STRING, region STRING, op STRING'
WEB_DDL = 'event_id STRING, event_time STRING, page STRING'
COLUMNS = ['event_id','customer_id','sequence','effective_at','customer_name','email','region','op']

def create_tables():
    # Reuse the original four schemas and ADLS volume; create only new tables.
    for layer in ['bronze','silver','gold','ops']:
        assert spark.catalog.databaseExists(f'{CAT}.{PREFIX}_{layer}'), 'Run original setup first'
    for entity,ddl in [('customer_changes',CUSTOMER_DDL),('web_events',WEB_DDL)]:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {name('bronze',entity)} ({ddl}) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        dbutils.fs.mkdirs(f'{ROOT}/landing/{entity}')
    typed = CUSTOMER_DDL.replace('effective_at STRING','effective_at TIMESTAMP')
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name('silver','customer_events')} ({typed}, payload_hash STRING) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name('silver','customer_contacts')} ({typed}, payload_hash STRING, is_deleted BOOLEAN) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name('silver','customer_profile_history')} (customer_id STRING, sequence BIGINT, region STRING, is_deleted BOOLEAN, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, customer_version_key STRING) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name('ops','customer_quarantine')} ({CUSTOMER_DDL}, reject_reason STRING, record_hash STRING) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name('ops','cdf_batches')} (batch_id BIGINT, source_rows BIGINT, distinct_valid_events BIGINT, rejected_rows BIGINT) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name('ops','stream_progress')} (phase INT, stream STRING, batch_id BIGINT, input_rows BIGINT, watermark STRING, dropped_by_watermark BIGINT, progress_json STRING) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name('ops','phase_status')} (phase INT, completed_at TIMESTAMP) USING DELTA")
    # Avoid automatic maintenance compute for this tiny learning dataset.
    # The caller owns these newly created managed tables.
    for layer,entities in [('bronze',['customer_changes','web_events']),('silver',['customer_events','customer_contacts','customer_profile_history']),('ops',['customer_quarantine','cdf_batches','stream_progress','phase_status'])]:
        for entity in entities:
            spark.sql(f"ALTER TABLE {name(layer,entity)} DISABLE PREDICTIVE OPTIMIZATION")

def land(phase):
    for entity,rows in [('customer_changes',phase['customers']),('web_events',phase['web'])]:
        if not rows:
            continue
        path=f'{ROOT}/landing/{entity}/phase_{phase["phase"]:02d}.json'
        content='\n'.join(json.dumps(r,sort_keys=True) for r in rows)+'\n'
        existing={f.name for f in dbutils.fs.ls(f'{ROOT}/landing/{entity}')}
        if path.rsplit('/',1)[1] not in existing:
            dbutils.fs.put(path,content,overwrite=False)
        else:
            assert dbutils.fs.head(path,1000000)==content, 'A landed file was changed; use a new demo_id'

def ingest_bronze():
    for entity,ddl in [('customer_changes',CUSTOMER_DDL),('web_events',WEB_DDL)]:
        query=(spark.readStream.format('cloudFiles').option('cloudFiles.format','json')
            .option('cloudFiles.schemaEvolutionMode','none').option('mode','FAILFAST')
            .schema(ddl).load(f'{ROOT}/landing/{entity}')
            .writeStream.format('delta').outputMode('append')
            .option('checkpointLocation',f'{ROOT}/checkpoints/bronze_{entity}')
            .trigger(availableNow=True).toTable(name('bronze',entity)))
        query.awaitTermination()

# Capture only strings in the callback closure; use the batch's own SparkSession.
# No dbutils or driver SparkSession is accessed in foreachBatch.
def customer_batch_handler(events_table,contacts_table,history_table,quarantine_table,audit_table):
    def process(batch,batch_id):
        from pyspark.sql import functions as F, Window
        from delta.tables import DeltaTable
        ss=batch.sparkSession
        raw=batch.filter("_change_type = 'insert'").select(*COLUMNS)
        source_rows=raw.count()
        if source_rows==0:
            return
        # Customer change envelope is append-only. Business deletes have op='D'.
        typed=(raw.withColumn('_raw_effective_at',F.col('effective_at'))
            .withColumn('effective_at',F.expr('try_cast(effective_at AS TIMESTAMP)')))
        envelope=(F.col('event_id').rlike(r'^[A-Za-z0-9_-]+$') & F.col('customer_id').rlike(r'^[A-Za-z0-9_-]+$')
            & (F.col('sequence')>0) & F.col('effective_at').isNotNull() & F.col('op').isin('U','D'))
        payload=(F.col('customer_name').isNotNull() & F.col('region').isNotNull()
            & F.col('email').rlike(r'^[^@\s]+@[^@\s]+\.[^@\s]+$'))
        valid=F.coalesce(envelope & ((F.col('op')=='D') | payload),F.lit(False))
        marked=typed.withColumn('_valid',valid)
        # Hash the raw record to retain invalid timestamp text and deduplicate replays.
        bad=(marked.filter('NOT _valid')
             .withColumn('effective_at',F.col('_raw_effective_at')).drop('_valid','_raw_effective_at')
             .withColumn('reject_reason',F.lit('Invalid customer envelope or payload'))
             .withColumn('record_hash',F.sha2(F.to_json(F.struct(*[F.col(c) for c in COLUMNS])),256))
             .dropDuplicates(['record_hash']))
        rejected=bad.count()
        (DeltaTable.forName(ss,quarantine_table).alias('t').merge(bad.alias('s'),'t.record_hash=s.record_hash')
            .whenNotMatchedInsertAll().execute())
        good=(marked.filter('_valid').drop('_valid','_raw_effective_at').dropDuplicates()
            .withColumn('payload_hash',F.sha2(F.to_json(F.struct(*[F.col(c) for c in COLUMNS])),256)))
        valid_count=good.count()
        if valid_count:
            if good.groupBy('event_id').count().filter('count>1').limit(1).count():
                raise ValueError('Conflicting duplicate event_id in incoming changes')
            if good.groupBy('customer_id','sequence').count().filter('count>1').limit(1).count():
                raise ValueError('Conflicting customer sequence in incoming changes')
            old=ss.table(events_table).alias('o')
            conflict=(good.alias('n').join(old,F.col('n.event_id')==F.col('o.event_id'))
                .filter(F.col('n.payload_hash')!=F.col('o.payload_hash')))
            if conflict.limit(1).count():
                raise ValueError('event_id reused with different content')
            conflict=(good.alias('n').join(old,(F.col('n.customer_id')==F.col('o.customer_id')) & (F.col('n.sequence')==F.col('o.sequence')))
                .filter(F.col('n.payload_hash')!=F.col('o.payload_hash')))
            if conflict.limit(1).count():
                raise ValueError('Existing customer sequence has different content')
            keys=good.select('customer_id').distinct()
            key_values=[r[0] for r in keys.limit(10001).collect()]
            if len(key_values)>10000:
                raise ValueError('Demo batch exceeds 10,000 affected customers; redesign the scoped history write')
            # Validate chronological contract BEFORE committing the event ledger.
            combined=(ss.table(events_table).join(keys,'customer_id','left_semi').unionByName(good).dropDuplicates(['event_id']))
            order=Window.partitionBy('customer_id').orderBy('sequence')
            if (combined.withColumn('_prev',F.lag('effective_at').over(order))
                .filter(F.col('effective_at')<=F.col('_prev')).limit(1).count()):
                raise ValueError('Effective time must strictly increase with sequence within a customer')
            (DeltaTable.forName(ss,events_table).alias('t').merge(good.alias('s'),'t.event_id=s.event_id')
                .whenNotMatchedInsertAll().execute())
            # If a previous attempt failed after the ledger commit, this recomputes
            # the same customer outputs from the idempotent ledger.
            affected=ss.table(events_table).join(keys,'customer_id','left_semi')
            latest=(affected.withColumn('_rn',F.row_number().over(Window.partitionBy('customer_id').orderBy(F.col('sequence').desc())))
                .filter('_rn=1').drop('_rn').withColumn('is_deleted',F.col('op')=='D'))
            (DeltaTable.forName(ss,contacts_table).alias('t').merge(latest.alias('s'),'t.customer_id=s.customer_id')
                .whenMatchedUpdateAll(condition='s.sequence>t.sequence').whenNotMatchedInsertAll().execute())
            # Type 2 tracks region/deletion status only. Email-only changes do not
            # create a new historical version. Retain tombstones explicitly.
            history=(affected.withColumn('is_deleted',F.col('op')=='D')
                .withColumn('_state',F.to_json(F.struct('region','is_deleted')))
                .withColumn('_previous',F.lag('_state').over(order))
                .filter(F.col('_previous').isNull() | (F.col('_state')!=F.col('_previous'))))
            history=(history.withColumn('valid_from',F.col('effective_at'))
                .withColumn('valid_to',F.lead('effective_at').over(order))
                .withColumn('is_current',F.col('valid_to').isNull())
                .withColumn('customer_version_key',F.sha2(F.concat_ws('|','customer_id',F.col('sequence').cast('string')),256))
                .select('customer_id','sequence','region','is_deleted','valid_from','valid_to','is_current','customer_version_key'))
            # Contract restricts IDs to alphanumerics/_/- so this predicate is safe.
            predicate='customer_id IN ('+','.join("'"+k+"'" for k in key_values)+')'
            (history.write.format('delta').mode('overwrite').option('replaceWhere',predicate).saveAsTable(history_table))
        audit=ss.createDataFrame([(int(batch_id),source_rows,valid_count,rejected)],
            'batch_id long, source_rows long, distinct_valid_events long, rejected_rows long')
        (DeltaTable.forName(ss,audit_table).alias('t').merge(audit.alias('s'),'t.batch_id=s.batch_id')
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    return process

def process_customers():
    callback=customer_batch_handler(name('silver','customer_events'),name('silver','customer_contacts'),
        name('silver','customer_profile_history'),name('ops','customer_quarantine'),name('ops','cdf_batches'))
    query=(spark.readStream.option('readChangeFeed','true').table(name('bronze','customer_changes'))
        .writeStream.foreachBatch(callback).option('checkpointLocation',f'{ROOT}/checkpoints/customers_cdf')
        .trigger(availableNow=True).start())
    query.awaitTermination()

def record_progress(query,phase,label):
    # Missing serverless metrics remain NULL; missing batch IDs are not auditable.
    rows=[]
    for progress in (query.recentProgress or []):
        if isinstance(progress,str):
            progress=json.loads(progress)
        if not progress or progress.get('batchId') is None:
            continue
        counts=[s.get('numRowsDroppedByWatermark') for s in (progress.get('stateOperators') or [])]
        dropped=sum(int(v) for v in counts) if counts and all(v is not None for v in counts) else None
        input_rows=progress.get('numInputRows')
        rows.append((phase,label,int(progress['batchId']),
            int(input_rows) if input_rows is not None else None,
            (progress.get('eventTime') or {}).get('watermark',''),dropped,json.dumps(progress, default=str)))
    if rows:
        df=spark.createDataFrame(rows,'phase int, stream string, batch_id long, input_rows long, watermark string, dropped_by_watermark long, progress_json string')
        (DeltaTable.forName(spark,name('ops','stream_progress')).alias('t').merge(df.alias('s'),'t.stream=s.stream AND t.batch_id=s.batch_id')
            .whenNotMatchedInsertAll().execute())

def process_web(phase):
    raw=spark.readStream.table(name('bronze','web_events'))
    valid=(raw.withColumn('event_time',F.expr('try_cast(event_time AS TIMESTAMP)'))
        .filter('event_time IS NOT NULL AND event_id IS NOT NULL AND page IS NOT NULL'))
    # Dedicated query: one stateful operator, native Delta sink, stable checkpoint.
    unique=valid.withWatermark('event_time','10 minutes').dropDuplicatesWithinWatermark(['event_id'])
    query=(unique.writeStream.format('delta').outputMode('append')
        .option('checkpointLocation',f'{ROOT}/checkpoints/web_dedup')
        .trigger(availableNow=True).toTable(name('silver','web_events')))
    query.awaitTermination()
    record_progress(query,phase,'web_dedup')
    # Separate checkpoint/query avoids chaining two stateful operators.
    aggregate=(spark.readStream.table(name('silver','web_events'))
        .withWatermark('event_time','10 minutes')
        .groupBy(F.window('event_time','5 minutes'),'page').count()
        .select(F.col('window.start').alias('window_start'),F.col('window.end').alias('window_end'),'page',F.col('count').alias('event_count')))
    query=(aggregate.writeStream.format('delta').outputMode('append')
        .option('checkpointLocation',f'{ROOT}/checkpoints/web_aggregate')
        .trigger(availableNow=True).toTable(name('gold','web_activity_5m')))
    query.awaitTermination()
    record_progress(query,phase,'web_aggregate')

def assert_customer_results(phase):
    current=spark.table(name('silver','customer_contacts'))
    history=spark.table(name('silver','customer_profile_history'))
    assert current.count()==2
    assert current.select('customer_id').distinct().count()==2
    c1=current.filter("customer_id='C1'").first()
    if phase==1:
        assert c1.sequence==1 and c1.region=='North'
    elif phase==2:
        assert c1.sequence==3 and c1.region=='South'
    else:
        assert c1.sequence==4 and c1.email=='asha.final@example.com'
    if phase>=2:
        assert current.filter("customer_id='C2' AND is_deleted").count()==1
    if phase>=3:
        h=history.filter("customer_id='C1'").orderBy('valid_from').collect()
        assert [r.region for r in h]==['North','West','South']
        assert [r.sequence for r in h]==[1,2,3], 'Contact-only v4 must not add a region version'
        assert h[0].valid_to==h[1].valid_from and h[1].valid_to==h[2].valid_from
        assert h[2].valid_to is None and h[2].is_current
        assert spark.table(name('silver','customer_events')).count()==6
    assert not history.filter('is_current').groupBy('customer_id').count().filter('count!=1').limit(1).count()
    assert history.filter('is_current').count()==2
    assert not history.filter('valid_to IS NOT NULL AND valid_to<=valid_from').limit(1).count()
    if phase>=4:
        assert spark.table(name('ops','customer_quarantine')).filter("customer_id='C3'").count()==1

def final_checks():
    assert_customer_results(5)
    unique=spark.table(name('silver','web_events'))
    ids={r.event_id for r in unique.select('event_id').collect()}
    assert ids=={'e1','e2','e3','e4','e5','e6','e7'}, f'Unexpected accepted events: {ids}'
    assert unique.count()==7, 'Duplicate web events were counted more than once'
    windows=spark.table(name('gold','web_activity_5m'))
    rows=windows.orderBy('window_start','page').collect()
    observed={(r.window_start.strftime('%H:%M'),r.page):r.event_count for r in rows}
    assert observed=={('10:00','/shop'):3,('10:05','/shop'):1,('10:25','/checkout'):2}, observed
    # The final 10:45 window remains open until newer event time advances the watermark.
    audit=spark.table(name('ops','cdf_batches'))
    assert audit.agg(F.sum('source_rows')).first()[0]==8, 'CDF should process eight delivered changes, not rescan history'
    assert spark.table(name('bronze','web_events')).count()==10
    print('PASS: SCD1, SCD2, late history insertion, soft delete, quarantine, CDF, watermark and window totals')

def mark_phase(phase):
    df=spark.createDataFrame([(phase,datetime.utcnow())],'phase int, completed_at timestamp')
    (DeltaTable.forName(spark,name('ops','phase_status')).alias('t').merge(df.alias('s'),'t.phase=s.phase')
        .whenNotMatchedInsertAll().execute())
