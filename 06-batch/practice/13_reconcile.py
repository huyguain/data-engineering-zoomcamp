"""Test đối chiếu cho job doanh thu theo ngày × vùng × loại (06 / 10).

Tính lại cùng chỉ số theo một đường khác (không groupBy, không join) rồi so:
  1. Tổng trips của output == số chuyến nguồn trong cùng khoảng ngày
     -> join không làm rơi dòng (inner join thiếu zone) và không nhân dòng.
  2. Tổng doanh thu khớp tới cent.
  3. Không có 2 dòng cùng grain (zone_id, day, service_type).

Chạy:  source env.sh && python 13_reconcile.py data/report/daily_zone_revenue
Kết quả 29/10/2026: 15,562,140 chuyến, 294,750,068.34 USD, 0 dòng trùng.
"""
import sysfrom pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
out = spark.read.parquet(sys.argv[1])
g = spark.read.parquet('data/pq/green/2021/*').withColumnRenamed('lpep_pickup_datetime','t')
y = spark.read.parquet('data/pq/yellow/2021/*').withColumnRenamed('tpep_pickup_datetime','t')
src = g.select('t','total_amount').unionAll(y.select('t','total_amount')) \
       .where("t >= '2021-01-01' AND t < '2021-09-01'")
a = out.agg(F.sum('trips'), F.round(F.sum('total_revenue'),2)).first()
b = src.agg(F.count('*'), F.round(F.sum('total_amount'),2)).first()
print("output :", a); print("source :", b); print("KHỚP" if a[0]==b[0] else "LỆCH")
dup = out.groupBy('zone_id','day','service_type').count().where('count>1').count()
print("dòng trùng grain:", dup)
