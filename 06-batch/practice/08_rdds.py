"""Ngày 6 — bài 11: viết lại báo cáo doanh thu bằng RDD thay vì DataFrame.

Cùng một câu hỏi, ba cách viết — SQL, DataFrame, RDD — để so kết quả và thời gian:

    SELECT date_trunc('hour', lpep_pickup_datetime) AS hour,
           PULocationID AS zone,
           SUM(total_amount) AS revenue,
           COUNT(1) AS count
    FROM green WHERE lpep_pickup_datetime >= '2021-01-01' GROUP BY 1, 2
"""
import os, sys, time
from collections import namedtuple
from datetime import datetime

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession, types, functions as F

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("day-06-rdds")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Để nguyên múi giờ mặc định của máy. Lý do xem Bước 8 — đặt
# spark.sql.session.timeZone ở đây sẽ làm RDD và SQL ra hai kết quả khác nhau.
jvm_tz = spark.sparkContext._jvm.java.util.TimeZone.getDefault().getID()
print("múi giờ JVM:", jvm_tz, "| session:", spark.conf.get("spark.sql.session.timeZone"))

df_green = spark.read.parquet("data/pq/green/2021/*")
start = datetime(year=2021, month=1, day=1)

def timed(label, fn, runs=2):
    times = []
    for _ in range(runs):
        t0 = time.time()
        out = fn()
        times.append(time.time() - t0)
    print(f"[{label}] " + " · ".join(f"{t:.2f}s" for t in times))
    return out

# --- Bước 1: DataFrame xuống RDD -----------------------------------------
# df.rdd là RDD của các đối tượng Row — chính là thứ DataFrame xây bên trên.
print("một Row trông như thế nào:")
print(df_green.rdd.take(1)[0])

# Chỉ giữ 3 cột cần dùng. Làm việc này TRƯỚC khi .rdd là quan trọng:
# sau khi xuống RDD, Catalyst hết đường cắt cột giúp mình.
rdd = (
    df_green
    .select("lpep_pickup_datetime", "PULocationID", "total_amount")
    .rdd
)
print("RDD có", rdd.getNumPartitions(), "partition")

# --- Bước 2: WHERE → filter ----------------------------------------------
def filter_outliers(row):
    return row.lpep_pickup_datetime >= start

# --- Bước 3: SELECT + GROUP BY key → map thành cặp (key, value) ----------
def prepare_for_grouping(row):
    hour = row.lpep_pickup_datetime.replace(minute=0, second=0, microsecond=0)
    zone = row.PULocationID
    amount = row.total_amount or 0.0
    return ((hour, zone), (amount, 1))

# --- Bước 4: SUM/COUNT → reduceByKey -------------------------------------
# Spark gọi hàm này theo chuỗi: reduce 2 giá trị đầu, lấy kết quả reduce tiếp
# với giá trị thứ 3... nên đầu vào và đầu ra PHẢI cùng kiểu (amount, count).
def calculate_revenue(left, right):
    left_amount, left_count = left
    right_amount, right_count = right
    return (left_amount + right_amount, left_count + right_count)

print("\nsau filter → map → reduceByKey, 3 bản ghi đầu:")
for r in rdd.filter(filter_outliers).map(prepare_for_grouping).reduceByKey(calculate_revenue).take(3):
    print("  ", r)

# --- Bước 5: tháo cấu trúc lồng rồi quay lại DataFrame --------------------
RevenueRow = namedtuple("RevenueRow", ["hour", "zone", "revenue", "count"])

def unwrap(row):
    return RevenueRow(hour=row[0][0], zone=row[0][1], revenue=row[1][0], count=row[1][1])

# Khai báo schema sẵn: không khai thì Spark phải chạy một job phụ chỉ để đoán kiểu.
result_schema = types.StructType([
    types.StructField("hour", types.TimestampType(), True),
    types.StructField("zone", types.IntegerType(), True),
    types.StructField("revenue", types.DoubleType(), True),
    types.StructField("count", types.IntegerType(), True),
])

df_rdd_result = (
    rdd
    .filter(filter_outliers)
    .map(prepare_for_grouping)
    .reduceByKey(calculate_revenue)
    .map(unwrap)
    .toDF(result_schema)
)

# --- Bước 6: so với DataFrame API và SQL ---------------------------------
df_green.createOrReplaceTempView("green")

df_api_result = (
    df_green
    .filter(df_green.lpep_pickup_datetime >= F.lit(start))
    .groupBy(
        F.date_trunc("hour", "lpep_pickup_datetime").alias("hour"),
        F.col("PULocationID").alias("zone"),
    )
    .agg(F.sum("total_amount").alias("revenue"), F.count(F.lit(1)).alias("count"))
)

df_sql_result = spark.sql("""
    SELECT DATE_TRUNC('hour', lpep_pickup_datetime) AS hour,
           PULocationID AS zone,
           SUM(total_amount) AS revenue,
           COUNT(1) AS count
    FROM green
    WHERE lpep_pickup_datetime >= '2021-01-01'
    GROUP BY 1, 2
""")

print()
n_rdd = timed("RDD      ", lambda: df_rdd_result.count())
n_api = timed("DataFrame", lambda: df_api_result.count())
n_sql = timed("SQL      ", lambda: df_sql_result.count())
print("số dòng:", n_rdd, n_api, n_sql, "→ khớp" if n_rdd == n_api == n_sql else "→ LỆCH")

# Tổng doanh thu phải bằng nhau tới từng xu
total_rdd = df_rdd_result.agg(F.sum("revenue")).first()[0]
total_sql = df_sql_result.agg(F.sum("revenue")).first()[0]
print(f"tổng doanh thu: RDD {total_rdd:,.2f} | SQL {total_sql:,.2f}")

# --- Bước 7: nhìn plan — RDD không có plan ---------------------------------
print("\n--- plan của bản SQL (Catalyst biết rõ từng cột, từng phép tính) ---")
df_sql_result.explain(mode="simple")

print("\n--- plan của bản RDD (Catalyst chỉ thấy 'có một RDD nào đó') ---")
df_rdd_result.explain(mode="simple")

print("\n--- chuỗi phụ thuộc thật của RDD (toDebugString) ---")
print(
    rdd.filter(filter_outliers)
       .map(prepare_for_grouping)
       .reduceByKey(calculate_revenue)
       .toDebugString()
       .decode()
)

# --- Bước 8: cái bẫy múi giờ khi xuống RDD -------------------------------
# spark.sql.session.timeZone chỉ chi phối phía SQL/Catalyst. Khi dữ liệu
# chuyển sang Python, Spark dựng datetime theo múi giờ MẶC ĐỊNH CỦA JVM.
# Đổi session timezone → bản SQL đổi kết quả, bản RDD đứng yên.
print("\n--- Bước 8: đổi session timezone, xem bên nào nhúc nhích ---")
one = df_green.select("lpep_pickup_datetime").orderBy("lpep_pickup_datetime")
for tz in [jvm_tz, "America/New_York", "UTC"]:
    spark.conf.set("spark.sql.session.timeZone", tz)
    sql_view = one.selectExpr("date_format(lpep_pickup_datetime,'yyyy-MM-dd HH:mm:ss')").first()[0]
    py_view = one.rdd.take(1)[0][0]
    n = spark.sql("""
        SELECT COUNT(1) FROM green WHERE lpep_pickup_datetime >= '2021-01-01'
    """).first()[0]
    print(f"  tz={tz:17} SQL thấy {sql_view} | Python nhận {py_view} | WHERE lọc còn {n:,}")

spark.conf.set("spark.sql.session.timeZone", jvm_tz)
print(f"  → phía Python không đổi: luôn theo JVM ({jvm_tz}).")
print("    Muốn RDD và SQL khớp nhau thì hai múi giờ phải trùng, hoặc xử lý")
print("    thời gian bằng UTC tường minh trước khi xuống RDD.")

spark.stop()
