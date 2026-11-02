"""Ngày 2 Phần 4 Mức 1: Spark đọc file từ HDFS, số block có quyết định số task không?

Chuẩn bị (xem my-study-plan/focus/day2-spark-hadoop-practice.md, Phần 4):
  hdfs dfs -mkdir -p /taxi && hdfs dfs -put data/raw/fhvhv_tripdata_2021-01.csv /taxi/
  hdfs fsck /taxi/fhvhv_tripdata_2021-01.csv -files -blocks -locations

Chạy:
  source env.sh
  spark-submit --master yarn --num-executors 2 --executor-cores 2 --executor-memory 2g \\
    12_hdfs_probe.py hdfs://localhost:9000/taxi/fhvhv_tripdata_2021-01.csv
"""
import json
import os
import sys
import urllib.request
from collections import Counter

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession

PATH = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/taxi/fhvhv_tripdata_2021-01.csv"

spark = SparkSession.builder.appName("hdfs-probe").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
print("master:", sc.master, "| app:", sc.applicationId)


def stages_since(seen):
    """Stage mới hoàn tất kể từ lần gọi trước, kèm số task theo locality."""
    # Trên YARN, Spark UI gắn AmIpFilter: gọi thẳng :4040 bị chuyển hướng ra
    # trang HTML. Phải đi qua proxy của ResourceManager.
    ui = (f"http://localhost:8088/proxy/{sc.applicationId}"
          if sc.master.startswith("yarn") else sc.uiWebUrl)
    base = f"{ui}/api/v1/applications/{sc.applicationId}"
    stages = json.load(urllib.request.urlopen(f"{base}/stages?status=complete"))
    out = []
    for st in sorted(stages, key=lambda s: s["stageId"]):
        if st["stageId"] in seen:
            continue
        seen.add(st["stageId"])
        tasks = json.load(urllib.request.urlopen(
            f"{base}/stages/{st['stageId']}/{st['attemptId']}/taskList?length=1000"))
        loc = Counter(t["taskLocality"] for t in tasks)
        out.append((st["stageId"], st["numTasks"], dict(loc)))
    return out


seen = set()

# 1. RDD: dùng InputFormat của Hadoop, mỗi split = 1 block.
rdd = sc.textFile(PATH)
print("\n[1] sc.textFile           -> partitions:", rdd.getNumPartitions(), "| dòng:", rdd.count())
print("    stage:", stages_since(seen))

# 2. DataFrame: split theo spark.sql.files.maxPartitionBytes (mặc định 128 MB),
#    trùng với block size nên cũng ra 6.
df = spark.read.option("header", "true").csv(PATH)
print("\n[2] DataFrame, maxPartitionBytes=128MB -> partitions:", df.rdd.getNumPartitions(),
      "| dòng:", df.count())
print("    stage:", stages_since(seen))

# 3. Hạ split xuống 64 MB: vẫn 6 block nhưng thành 12 task.
#    Số task do kích thước split quyết định; block chỉ quyết định task nên chạy ở máy nào.
spark.conf.set("spark.sql.files.maxPartitionBytes", str(64 * 1024**2))
df64 = spark.read.option("header", "true").csv(PATH)
print("\n[3] DataFrame, maxPartitionBytes=64MB  -> partitions:", df64.rdd.getNumPartitions(),
      "| dòng:", df64.count())
print("    stage:", stages_since(seen))

spark.stop()
