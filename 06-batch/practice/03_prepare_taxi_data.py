"""Bước 8 - Chuẩn hoá dữ liệu taxi green/yellow.

Đọc từng tháng CSV.gz với schema tự khai báo, ghi ra parquet theo cấu trúc
data/pq/<loại>/<năm>/<tháng>/. Mục đích: mọi tháng có CÙNG schema, nên ghép
lại không bị lệch kiểu.

Chạy:  source env.sh && python 03_prepare_taxi_data.py
"""

import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession, types

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("prepare-taxi-data")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# --- Schema: thứ tự cột phải khớp tuyệt đối với file CSV ------------------
# green đặt store_and_fwd_flag ở vị trí 4, yellow ở vị trí 7 -> hai schema riêng.

green_schema = types.StructType([
    types.StructField('VendorID',              types.IntegerType(),   True),
    types.StructField('lpep_pickup_datetime',  types.TimestampType(), True),
    types.StructField('lpep_dropoff_datetime', types.TimestampType(), True),
    types.StructField('store_and_fwd_flag',    types.StringType(),    True),
    types.StructField('RatecodeID',            types.IntegerType(),   True),
    types.StructField('PULocationID',          types.IntegerType(),   True),
    types.StructField('DOLocationID',          types.IntegerType(),   True),
    types.StructField('passenger_count',       types.IntegerType(),   True),
    types.StructField('trip_distance',         types.DoubleType(),    True),
    types.StructField('fare_amount',           types.DoubleType(),    True),
    types.StructField('extra',                 types.DoubleType(),    True),
    types.StructField('mta_tax',               types.DoubleType(),    True),
    types.StructField('tip_amount',            types.DoubleType(),    True),
    types.StructField('tolls_amount',          types.DoubleType(),    True),
    types.StructField('ehail_fee',             types.DoubleType(),    True),
    types.StructField('improvement_surcharge', types.DoubleType(),    True),
    types.StructField('total_amount',          types.DoubleType(),    True),
    types.StructField('payment_type',          types.IntegerType(),   True),
    types.StructField('trip_type',             types.IntegerType(),   True),
    types.StructField('congestion_surcharge',  types.DoubleType(),    True),
])

yellow_schema = types.StructType([
    types.StructField('VendorID',              types.IntegerType(),   True),
    types.StructField('tpep_pickup_datetime',  types.TimestampType(), True),
    types.StructField('tpep_dropoff_datetime', types.TimestampType(), True),
    types.StructField('passenger_count',       types.IntegerType(),   True),
    types.StructField('trip_distance',         types.DoubleType(),    True),
    types.StructField('RatecodeID',            types.IntegerType(),   True),
    types.StructField('store_and_fwd_flag',    types.StringType(),    True),
    types.StructField('PULocationID',          types.IntegerType(),   True),
    types.StructField('DOLocationID',          types.IntegerType(),   True),
    types.StructField('payment_type',          types.IntegerType(),   True),
    types.StructField('fare_amount',           types.DoubleType(),    True),
    types.StructField('extra',                 types.DoubleType(),    True),
    types.StructField('mta_tax',               types.DoubleType(),    True),
    types.StructField('tip_amount',            types.DoubleType(),    True),
    types.StructField('tolls_amount',          types.DoubleType(),    True),
    types.StructField('improvement_surcharge', types.DoubleType(),    True),
    types.StructField('total_amount',          types.DoubleType(),    True),
    types.StructField('congestion_surcharge',  types.DoubleType(),    True),
])

# --- Vòng lặp chuyển đổi ---------------------------------------------------

JOBS = [
    ("green",  2021, range(1, 13), green_schema),
    ("yellow", 2021, range(1, 13), yellow_schema),
]

for taxi_type, year, months, schema in JOBS:
    for month in months:
        input_path  = f"data/raw/{taxi_type}/{year}/{month:02d}/"
        output_path = f"data/pq/{taxi_type}/{year}/{month:02d}/"

        # Tháng 8-12 không tồn tại -> bỏ qua, không để chết cả job
        if not os.path.isdir(input_path):
            print(f"bỏ qua {input_path} (không có dữ liệu)")
            continue

        df = (
            spark.read
            .option("header", "true")
            .schema(schema)
            .csv(input_path)
        )

        # .gz không chia được -> chỉ 1 task đọc.
        # repartition(4) để bước GHI dùng được nhiều core.
        df.repartition(4).write.mode("overwrite").parquet(output_path)
        print(f"{taxi_type} {year}-{month:02d}: {df.count():>9,} dòng -> {output_path}")

# --- Kiểm tra: đọc cả năm bằng một lệnh ------------------------------------

df_green_all = spark.read.parquet("data/pq/green/2021/*")
print(f"\nĐọc cả năm green 2021 bằng 1 lệnh: {df_green_all.count():,} dòng, "
      f"{df_green_all.rdd.getNumPartitions()} partition")
df_green_all.printSchema()

spark.stop()
