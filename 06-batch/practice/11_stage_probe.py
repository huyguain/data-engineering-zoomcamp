"""Ngày 2 ôn phỏng vấn — đo stage, shuffle, chiến lược join, AQE, RDD vs DataFrame.

Chạy:  source env.sh  &&  python 11_stage_probe.py data/report/probe
In ra từng stage của job daily zone revenue (số task, shuffle write/read) lấy từ
REST API của Spark UI, trong 3 cấu hình: broadcast + AQE, sort-merge + AQE,
sort-merge không AQE. Cuối cùng so sánh DataFrame groupBy với RDD reduceByKey/groupByKey.
"""
import os, sys, time, json, urllib.request
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[*]").appName("probe").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
sc = spark.sparkContext
OUT = sys.argv[1] if len(sys.argv) > 1 else "data/report/probe"
print("defaultParallelism", sc.defaultParallelism)

def stages():
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages"
    return json.load(urllib.request.urlopen(url))

def build():
    g = (spark.read.parquet('data/pq/green/2021/*')
         .withColumnRenamed('lpep_pickup_datetime','pickup_datetime'))
    y = (spark.read.parquet('data/pq/yellow/2021/*')
         .withColumnRenamed('tpep_pickup_datetime','pickup_datetime'))
    cols = ['pickup_datetime','PULocationID','trip_distance','total_amount']
    trips = (g.select(cols).withColumn('service_type',F.lit('green'))
             .unionByName(y.select(cols).withColumn('service_type',F.lit('yellow'))))
    print("input partitions green/yellow:", g.rdd.getNumPartitions(), y.rdd.getNumPartitions())
    daily = (trips.where((F.col('pickup_datetime')>='2021-01-01')&(F.col('pickup_datetime')<'2021-09-01'))
             .groupBy(F.col('PULocationID').alias('zone_id'), F.date_trunc('day','pickup_datetime').alias('day'),'service_type')
             .agg(F.count('*').alias('trips'), F.sum('total_amount').alias('rev'), F.avg('trip_distance').alias('avg_dist')))
    zones = spark.read.option("header","true").option("inferSchema","true").csv('data/raw/zones/taxi_zone_lookup.csv')
    return daily, zones

def run(label, join_hint=True):
    before = {s['stageId'] for s in stages()}
    daily, zones = build()
    res = daily.join(F.broadcast(zones) if join_hint else zones, daily.zone_id == zones.locationid)
    t=time.time(); res.write.mode('overwrite').parquet(f"{OUT}/{label}"); dt=time.time()-t
    new = sorted([s for s in stages() if s['stageId'] not in before and s['status']=='COMPLETE'], key=lambda s:s['stageId'])
    print(f"\n=== {label}: {dt:.1f}s, {len(new)} stages")
    for s in new:
        print(f"  stage {s['stageId']}: tasks={s['numTasks']:>3} in={s['inputBytes']/1e6:7.1f}MB "
              f"shuffleW={s['shuffleWriteBytes']/1e6:6.1f}MB shuffleR={s['shuffleReadBytes']/1e6:6.1f}MB "
              f"recIn={s['inputRecords']:>9} | {s['name'][:50]}")
    return res

r = run("broadcast_aqe_on")
r.explain()  # final plan after action
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
run("sortmerge_aqe_on", join_hint=False)
spark.conf.set("spark.sql.adaptive.enabled", "false")
run("sortmerge_aqe_off", join_hint=False)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

# RDD vs DataFrame: tổng doanh thu theo zone trên yellow
y = spark.read.parquet('data/pq/yellow/2021/*').select('PULocationID','total_amount').where('total_amount is not null')
t=time.time(); n1=y.groupBy('PULocationID').agg(F.sum('total_amount')).count(); d1=time.time()-t
rdd = y.rdd.map(lambda r:(r.PULocationID, r.total_amount))
t=time.time(); n2=rdd.reduceByKey(lambda a,b:a+b).count(); d2=time.time()-t
t=time.time(); n3=rdd.groupByKey().mapValues(sum).count(); d3=time.time()-t
print(f"\nDataFrame groupBy: {d1:.1f}s ({n1} zones) | RDD reduceByKey: {d2:.1f}s ({n2}) | RDD groupByKey: {d3:.1f}s ({n3})")
spark.stop()
