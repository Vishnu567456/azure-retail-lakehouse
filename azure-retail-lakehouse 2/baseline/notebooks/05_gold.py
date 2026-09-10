# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

def active(entity):
    return spark.table(table('silver',entity)).filter('NOT is_deleted')
c = active('customers').alias('c')
r = active('regions').alias('r')
p = active('products').alias('p')
o = active('orders').alias('o')
# Fail before publishing if dimension references are unresolved. Rerun after dimensions arrive.
if c.join(r,F.col('c.region_id')==F.col('r.region_id'),'left_anti').limit(1).count():
    raise ValueError('Customer references missing region')
if o.join(c,F.col('o.customer_id')==F.col('c.customer_id'),'left_anti').limit(1).count():
    raise ValueError('Order references missing customer')
if o.join(p,F.col('o.product_id')==F.col('p.product_id'),'left_anti').limit(1).count():
    raise ValueError('Order references missing product')
customer_dim = c.join(r,F.col('c.region_id')==F.col('r.region_id')).select(
    'c.customer_id','c.customer_name','c.email','c.region_id','r.region_name')
product_dim = p.select('product_id','product_name','category')
fact = o.select('order_id','customer_id','product_id','order_date','quantity','unit_price',
    (F.col('quantity')*F.col('unit_price')).cast('decimal(20,2)').alias('revenue'))
daily = fact.groupBy('order_date').agg(F.count('*').alias('order_count'),
    F.sum('quantity').alias('units'),F.sum('revenue').alias('revenue'))
for name,df in [('dim_customers',customer_dim),('dim_products',product_dim),('fact_orders',fact),('daily_sales',daily)]:
    df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(table('gold',name))
    audit(name,'gold_total',df.count())
# Publish marker is written only after all four tables succeed.
audit('all','gold_publish_complete',1)
