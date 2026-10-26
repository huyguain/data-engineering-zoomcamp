"""Ngày 6 — bài 12: mapPartitions — xử lý cả một partition một lượt.

map    : 1 dòng  vào → 1 dòng  ra
mapPartitions: 1 partition vào → 1 partition ra (hàm nhận iterator, trả iterator)

Dùng khi hàm cần nhiều dòng cùng lúc: nạp model một lần rồi predict theo lô,
mở một kết nối DB cho cả partition thay vì mở lại từng dòng.
"""
import os, sys, time

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

import pandas as pd
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("day-06-map-partitions")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

columns = ["VendorID", "lpep_pickup_datetime", "PULocationID", "DOLocationID", "trip_distance"]

df_green = spark.read.parquet("data/pq/green/2021/*")
duration_rdd = df_green.select(columns).rdd
print("RDD có", duration_rdd.getNumPartitions(), "partition")

# --- Bước 1: hàm chạy MẤY lần? -------------------------------------------
# Trả về list 1 phần tử cho mỗi partition. Số 1 đếm được = số partition.
def count_calls(partition):
    return [1]

print("mapPartitions(lambda p: [1]) →", duration_rdd.mapPartitions(count_calls).collect())
print("  (so với map: ra 570.466 số 1 — mỗi dòng một lần gọi)")

# --- Bước 2: partition là iterator, không phải list -----------------------
# len(partition) sẽ lỗi: nó là itertools.chain. Chỉ duyệt được MỘT lần.
def partition_size(partition):
    cnt = 0
    for _ in partition:
        cnt += 1
    return [cnt]

sizes = duration_rdd.mapPartitions(partition_size).collect()
print("số dòng từng partition:", sizes)
print(f"  lệch {max(sizes) / min(sizes):.1f} lần → executor nhận partition to "
      f"vẫn chạy khi các executor khác đã rảnh")

# --- Bước 3: biến mỗi partition thành pandas DataFrame --------------------
# Cảnh báo bộ nhớ: pd.DataFrame(rows) nạp TOÀN BỘ partition vào RAM của
# executor. Partition quá to thì phải repartition hoặc cắt nhỏ iterator.
def model_predict(df):
    # Giả vờ là model đã train: 5 phút cho mỗi dặm.
    return df.trip_distance * 5

def apply_model_in_batch(rows):
    df = pd.DataFrame(rows, columns=columns)
    df["predicted_duration"] = model_predict(df)
    # KHÔNG return df được — mapPartitions cần iterator của từng dòng.
    for row in df.itertuples():
        yield row

df_predicts = (
    duration_rdd
    .mapPartitions(apply_model_in_batch)
    .toDF()
    .drop("Index")   # cột thừa do itertuples() sinh ra
)
df_predicts.select("PULocationID", "trip_distance", "predicted_duration").show(5)

# --- Bước 4: so với map từng dòng ----------------------------------------
# Cùng phép tính, nhưng hàm được gọi 570.466 lần thay vì 10 lần.
def predict_one_row(row):
    d = row.asDict()
    d["predicted_duration"] = (row.trip_distance or 0.0) * 5
    return d

t0 = time.time()
n_mp = duration_rdd.mapPartitions(apply_model_in_batch).count()
t_mp = time.time() - t0

t0 = time.time()
n_map = duration_rdd.map(predict_one_row).count()
t_map = time.time() - t0

print(f"[mapPartitions + pandas] {t_mp:.2f}s  ({n_mp:,} dòng)")
print(f"[map từng dòng]          {t_map:.2f}s  ({n_map:,} dòng)")

# --- Bước 5: cái giá của việc gọi lại mỗi dòng ---------------------------
# Giả lập chi phí cố định: "nạp model" tốn 20ms mỗi lần gọi hàm.
SETUP_COST = 0.02

def heavy_setup_per_partition(rows):
    time.sleep(SETUP_COST)          # nạp model 1 lần cho cả partition
    for row in rows:
        yield (row.trip_distance or 0.0) * 5

def heavy_setup_per_row(row):
    time.sleep(SETUP_COST)          # nạp model lại cho TỪNG dòng
    return (row.trip_distance or 0.0) * 5

small = duration_rdd.sample(False, 0.0005, seed=42).cache()
n_small = small.count()

t0 = time.time()
small.mapPartitions(heavy_setup_per_partition).count()
t_partition = time.time() - t0

t0 = time.time()
small.map(heavy_setup_per_row).count()
t_row = time.time() - t0

print(f"\ntrên {n_small:,} dòng, mỗi lần 'nạp model' tốn {SETUP_COST * 1000:.0f}ms:")
print(f"  mapPartitions: {t_partition:.2f}s  (nạp {duration_rdd.getNumPartitions()} lần)")
print(f"  map          : {t_row:.2f}s  (nạp {n_small:,} lần)")

spark.stop()
