import time
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("dataFrames")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

PQ = "data/pq/2021/01"

df = spark.read.parquet(PQ)
df.printSchema()

t0 = time.time()
q = df.select("pickup_datetime", "PULocationID", "hvfhs_license_num") \
    .filter(df.hvfhs_license_num == "HV0003")
print(f"select + filter: {time.time() - t0:.3f}s")

t0 = time.time()
q.show(5, truncate=False)
print(f"show: {time.time() - t0:.2f}s")

q.explain()

print("Số bản ghi:", q.count())
 # 2. Tự viết: thêm pickup_date và dropoff_date bằng F.to_date,
  #    select 3 cột đó + PULocationID rồi show(5)
t0 = time.time()
q2 = df.select(F.to_date("pickup_datetime").alias("pickup_date"), F.to_date("dropoff_datetime").alias("dropoff_date"), "PULocationID") \
    .filter(df.hvfhs_license_num == "HV0003")
print(f"select + filter: {time.time() - t0:.3f}s")

t0 = time.time()
q2.show(5, truncate=False)
print(f"show: {time.time() - t0:.2f}s")

spark.stop()
