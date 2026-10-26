"""Ngày 6 — bài 08: giải phẫu một cụm Spark, quan sát từ máy local.

Chạy hai lần để so sánh số core:

    python 07_cluster_anatomy.py --cores 1
    python 07_cluster_anatomy.py --cores 8
"""
import argparse, os, socket, time, sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--cores", default="*", help="số core cho local[N]; '*' = tất cả")
parser.add_argument("--input", default="data/pq/green/2021/*")
args = parser.parse_args()

spark = (
    SparkSession.builder
    .master(f"local[{args.cores}]")
    .appName(f"day-06-anatomy-local-{args.cores}")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# --- Bước 1: driver đang là ai, master đang là ai -------------------------
print("=" * 62)
print("DRIVER  pid           :", os.getpid())
print("DRIVER  host          :", socket.gethostname())
print("master                :", sc.master)
print("applicationId         :", sc.applicationId)
print("Spark UI (cổng 4040)  :", sc.uiWebUrl)
print("defaultParallelism    :", sc.defaultParallelism)
print("spark version         :", sc.version)

# Trong local mode, driver và executor nằm chung một JVM. getExecutorInfos()
# vẫn trả về danh sách — chỉ có đúng một phần tử tên "driver".
infos = sc._jsc.sc().statusTracker().getExecutorInfos()
print("số executor đăng ký   :", len(infos))
for e in infos:
    # Spark 4 bỏ executorId() khỏi SparkExecutorInfo — chỉ còn host/port/cache.
    print("   executor @", e.host(), "| cache:", e.cacheSize(), "byte",
          "| task đang chạy:", e.numRunningTasks())

# --- Bước 2: partition là đơn vị chia việc -------------------------------
df = spark.read.parquet(args.input).select("PULocationID", "total_amount")
print("-" * 62)
print(f"{args.input} → {df.rdd.getNumPartitions()} partition khi vừa đọc")

# mapPartitionsWithIndex: mỗi partition chạy trong một python worker riêng.
# In pid ra để thấy TẬN MẮT bao nhiêu tiến trình thật sự làm việc.
WORK = 0.0   # giây công việc "nặng" giả lập thêm cho mỗi partition

def who_am_i(index, partition):
    t0 = time.time()
    cnt = 0
    for _ in partition:
        cnt += 1
    time.sleep(WORK)
    yield (index, cnt, os.getpid(), round(time.time() - t0, 2))

def run(rdd, label):
    t0 = time.time()
    rows = sorted(rdd.mapPartitionsWithIndex(who_am_i).collect())
    elapsed = time.time() - t0

    print(f"\n### {label}")
    print(f"{'partition':>9} | {'số dòng':>9} | {'pid worker':>10} | {'giây':>6}")
    for index, cnt, pid, secs in rows:
        print(f"{index:>9} | {cnt:>9,} | {pid:>10} | {secs:>6.2f}")

    pids = {r[2] for r in rows}
    counts = [r[1] for r in rows]
    cpu = sum(r[3] for r in rows)
    print(f"tổng dòng {sum(counts):,} | {len(pids)} python worker: {sorted(pids)}")
    print(f"to nhất/nhỏ nhất: {max(counts):,}/{min(counts):,}")
    print(f"tổng CPU các partition {cpu:.1f}s | thực tế local[{args.cores}] {elapsed:.1f}s"
          f" | tăng tốc {cpu / elapsed:.1f}×")

# (a) partition tự nhiên của file — Spark gộp file nhỏ, không phải 1 file 1 partition
run(df.rdd, "partition tự nhiên")

# (b) chia lại thành 8 phần bằng nhau — shuffle, nhưng mỗi core có việc
even = df.repartition(8).rdd.cache()
run(even, "sau repartition(8)")

# (c) mỗi partition "nặng" 1 giây: số core quyết định thời gian thực tế.
# 8 partition × 1s = 8s CPU. local[1] phải chờ đủ 8s, local[8] xong trong ~1s.
WORK = 1.0
run(even, "8 partition, mỗi partition nặng 1s")
print("=" * 62)

input(f"\nSpark UI đang mở tại {sc.uiWebUrl} — nhấn Enter để thoát...")
spark.stop()

