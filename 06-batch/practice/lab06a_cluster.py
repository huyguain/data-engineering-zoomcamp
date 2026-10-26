"""Ngày 6 phần A — giải phẫu cụm Spark nhìn từ local mode.

Chạy:  source env.sh  &&  python -i lab06a_cluster.py
Cờ -i giữ lại REPL sau khi chạy xong, để nghịch tiếp với spark/sc/df.
"""
import os, sys, time, glob

# Python mà executor dùng. Không set thì Spark gọi python 3.9 của macOS.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[8]")        # 8 core — nhớ con số này, lát nữa nó quay lại
    .appName("lab-ngay-6")
    .getOrCreate()
)
sc = spark.sparkContext        # SparkContext: cửa ngõ xuống tầng RDD
sc.setLogLevel("ERROR")        # bớt log WARN cho dễ đọc


# --- A1: bốn vai trong sơ đồ cụm đang nằm ở đâu? -------------------------
print("=" * 58)
print("driver pid        :", os.getpid())
print("master            :", sc.master)
print("applicationId     :", sc.applicationId)
print("Spark UI          :", sc.uiWebUrl)
print("defaultParallelism:", sc.defaultParallelism)
print("=" * 58)

# --- A2: đọc dữ liệu, xem số lượng bản ghi và số phân vùng -------------------
df_green = spark.read.parquet("data/pq/green/2021/*").select("PULocationID", "total_amount")
print("green: ", df_green.count(), df_green.rdd.getNumPartitions(), "partitions")

globs = glob.glob("data/pq/green/2021/*/*.parquet")
print("Số lượng, size: ", len(globs), f"{sum(os.path.getsize(f) for f in globs) / 1e6:.2f} MB")

# sc.getConf() chỉ chứa những gì được set TƯỜNG MINH lúc tạo session, nên nó
# trả None. Giá trị mặc định của Spark SQL nằm ở spark.conf, không ở SparkConf.
print("Ngưỡng spark split: ", sc.getConf().get("spark.sql.files.maxPartitionBytes"), "(SparkConf — rỗng)")
print("Ngưỡng spark split: ", spark.conf.get("spark.sql.files.maxPartitionBytes"), "(spark.conf — thật)")


# --- A3: ai thật sự quyết định số partition? ----------------------------
print("-" * 58)
print("openCostInBytes   :", spark.conf.get("spark.sql.files.openCostInBytes"))

# minPartitionNum chưa set tường minh → hỏi thẳng sẽ ném INVALID_CONF_VALUE.
# Nó không có giá trị riêng: mặc định chính là defaultParallelism (= số core).
try:
    print("minPartitionNum   :", spark.conf.get("spark.sql.files.minPartitionNum"))
except Exception:
    print("minPartitionNum   : chưa set → dùng defaultParallelism =", sc.defaultParallelism)


def doc_lai(nhan):
    """Đọc lại từ đầu rồi đếm partition.

    Phải đọc lại chứ không dùng df_green cũ: số partition được chốt lúc Spark
    lập kế hoạch quét file, df_green đã chốt xong từ A2 rồi.
    """
    tmp = spark.read.parquet("data/pq/green/2021/*").select("PULocationID", "total_amount")
    print(f"  {nhan:<24} → {tmp.rdd.getNumPartitions():>3} partition")


print(f"\n{len(globs)} file · cùng một bộ dữ liệu, chỉ đổi config:")
doc_lai("mặc định (8 core)")

spark.conf.set("spark.sql.files.minPartitionNum", "2")
doc_lai("minPartitionNum = 2")

spark.conf.set("spark.sql.files.minPartitionNum", "32")
doc_lai("minPartitionNum = 32")

spark.conf.unset("spark.sql.files.minPartitionNum")   # trả lại mặc định
doc_lai("sau khi unset")
print("-" * 58)


# --- A4: bắt executor lộ mặt --------------------------------------------
# mapPartitionsWithIndex: hàm được gọi MỘT LẦN cho mỗi partition, nhận
# (số thứ tự partition, iterator các dòng). Ta bắt mỗi partition tự khai
# nó là ai và đang chạy trong tiến trình nào.
def who_am_i(index, partition):
    t0 = time.time()
    cnt = 0
    for _ in partition:        # partition là iterator — chỉ duyệt được MỘT lần
        cnt += 1
    yield (index, cnt, os.getpid(), round(time.time() - t0, 2))


ket_qua = sorted(df_green.rdd.mapPartitionsWithIndex(who_am_i).collect())

print(f"{'partition':>9} | {'số dòng':>9} | {'pid':>7} | {'giây':>5}")
for index, cnt, pid, giay in ket_qua:
    print(f"{index:>9} | {cnt:>9,} | {pid:>7} | {giay:>5.2f}")

pids = {r[2] for r in ket_qua}
so_dong = [r[1] for r in ket_qua]
print(f"\ndriver pid = {os.getpid()} — có nằm trong danh sách trên không?")
print(f"{len(pids)} pid khác nhau: {sorted(pids)}")
print(f"tổng {sum(so_dong):,} dòng | to nhất {max(so_dong):,} | nhỏ nhất {min(so_dong):,}")
print("=" * 58)


# --- A5: core là số chỗ ngồi, task phải xếp hàng theo đợt ---------------
# Giả lập mỗi partition tốn 1 giây xử lý. Tổng công việc tỉ lệ với số
# partition, nhưng số chỗ ngồi thì vẫn là 8.
def nang_1_giay(index, partition):
    cnt = sum(1 for _ in partition)
    time.sleep(1)
    yield (index, cnt)


def do_thoi_gian(rdd, nhan):
    n = rdd.getNumPartitions()
    t0 = time.time()
    rdd.mapPartitionsWithIndex(nang_1_giay).collect()
    giay = time.time() - t0
    print(f"  {nhan:<18} | {n:>2} partition = {n:>2}s CPU "
          f"| {n / sc.defaultParallelism:>4.1f} đợt | thực tế {giay:>5.2f}s")


print(f"\nmỗi partition nặng 1 giây, bạn có {sc.defaultParallelism} chỗ ngồi:")
do_thoi_gian(df_green.rdd, "như hiện tại")
do_thoi_gian(df_green.repartition(16).rdd, "repartition(16)")
do_thoi_gian(df_green.repartition(32).rdd, "repartition(32)")
print("=" * 58)