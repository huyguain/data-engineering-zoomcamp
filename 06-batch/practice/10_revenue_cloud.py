"""Doanh thu theo ngày × vùng × loại taxi, bản chạy được trên YARN, GCS, BigQuery.

Khác 06_daily_zone_revenue.py ở 3 chỗ:
  1. KHÔNG có .master(...) trong code. spark-submit --master quyết định
     chạy ở đâu. Cấu hình trong code thắng cấu hình dòng lệnh, nên để
     .master("local[*]") là job lặng lẽ chạy local dù gõ --master yarn.
  2. --format bigquery: ghi thẳng vào bảng BigQuery qua spark-bigquery connector.
  3. In ra master, applicationId, deploy mode và danh sách executor để thấy
     job thực sự chạy trên YARN.

Chạy trên YARN, đọc GCS, ghi BigQuery (xem my-study-plan/focus/day2-spark-hadoop-practice.md):
  source env.sh
  B=gs://de-zoomcamp-500308-nytaxi
  spark-submit --master yarn --deploy-mode client \\
    --num-executors 2 --executor-cores 2 --executor-memory 2g \\
    --jars lib/gcs-connector-3.1.9.jar \\
    --packages com.google.cloud.spark:spark-4.0-bigquery:0.46.0 \\
    10_revenue_cloud.py \\
      --input_green "$B/pq/green/2021/*" --input_yellow "$B/pq/yellow/2021/*" \\
      --zones "$B/zones/taxi_zone_lookup.csv" --timezone America/New_York \\
      --format bigquery --output nytaxi.daily_zone_revenue_yarn
"""

import argparse
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession, functions as F

# --- 1. Tham số dòng lệnh -------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument('--input_green',  required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--zones',        required=True)
parser.add_argument('--output',       required=True)
parser.add_argument('--format',       default='parquet', choices=['parquet', 'bigquery'])
parser.add_argument('--project',      default='de-zoomcamp-500308')
parser.add_argument('--temp_bucket',  default='de-zoomcamp-500308-nytaxi')
parser.add_argument('--date_from',    default='2021-01-01')
parser.add_argument('--date_to',      default='2021-09-01')   # loại trừ
parser.add_argument('--timezone',     default=None)
args = parser.parse_args()

# --- 2. Session: không có .master() ---------------------------------------
# Cấu hình gs:// đặt ở đây để lệnh spark-submit ngắn hơn. Jar connector vẫn
# phải đưa vào bằng --jars vì JVM đã khởi động trước khi dòng này chạy.

spark = (
    SparkSession.builder
    .appName("daily-zone-revenue-cloud")
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
    .config("spark.hadoop.fs.gs.project.id", args.project)
    # Hadoop 3.5 (đóng gói trong Spark 4.2) có sẵn mặc định fs.gs.* dạng "64m".
    # gcs-connector 3.1.9 đọc bằng getLong nên báo NumberFormatException: "64m".
    # Ghi đè bằng số byte thuần.
    .config("spark.hadoop.fs.gs.block.size", str(64 * 1024**2))
    .config("spark.hadoop.fs.gs.rewrite.max.chunk.size", str(512 * 1024**2))
    .config("spark.hadoop.fs.gs.outputstream.buffer.size", str(8 * 1024**2))
    .config("spark.hadoop.fs.gs.inputstream.inplace.seek.limit", str(8 * 1024**2))
    .config("spark.hadoop.fs.gs.inputstream.min.range.request.size", str(2 * 1024**2))
    # Parquet 1.17 đọc kiểu vectored; connector 3.1.9 khi đó gọi
    # VectoredReadUtils.validateRangeRequest của Hadoop 3.4, hàm này không còn
    # trong Hadoop 3.5 -> NoSuchMethodError. Quay về đọc tuần tự.
    .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")

if args.timezone:
    spark.conf.set("spark.sql.session.timeZone", args.timezone)

print("=" * 62)
print("master        :", sc.master)
print("deploy mode   :", sc.getConf().get("spark.submit.deployMode", "client"))
print("applicationId :", sc.applicationId)
print("Spark UI      :", sc.uiWebUrl)
print("múi giờ       :", spark.conf.get("spark.sql.session.timeZone"))
print("=" * 62)

# --- 3. Đọc, chuẩn hoá, gộp -----------------------------------------------

df_green = (
    spark.read.parquet(args.input_green)
    .withColumnRenamed('lpep_pickup_datetime',  'pickup_datetime')
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
)
df_yellow = (
    spark.read.parquet(args.input_yellow)
    .withColumnRenamed('tpep_pickup_datetime',  'pickup_datetime')
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
)

yellow_cols = set(df_yellow.columns)
common = [c for c in df_green.columns if c in yellow_cols]

trips = (
    df_green.select(common).withColumn('service_type', F.lit('green'))
    .unionAll(
    df_yellow.select(common).withColumn('service_type', F.lit('yellow')))
)
trips.createOrReplaceTempView('trips_data')

# --- 4. Gom theo ngày × vùng × loại ---------------------------------------

daily = spark.sql(f"""
SELECT
    PULocationID                        AS zone_id,
    date_trunc('day', pickup_datetime)  AS day,
    service_type,

    COUNT(1)                            AS trips,
    SUM(total_amount)                   AS total_revenue,
    AVG(trip_distance)                  AS avg_distance
FROM trips_data
WHERE pickup_datetime >= '{args.date_from}'
  AND pickup_datetime <  '{args.date_to}'
GROUP BY 1, 2, 3
""")

# --- 5. Gắn tên vùng bằng broadcast join ----------------------------------

zones = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(args.zones)
    .withColumnRenamed('zone', 'zone_name')
)

result = (
    daily
    .join(F.broadcast(zones), daily.zone_id == zones.locationid)
    .drop('locationid')
    .withColumnRenamed('Borough', 'borough')
)

# --- 6. Kiểm tra --------------------------------------------------------------

print("số dòng:", result.count())
result.orderBy(F.desc('total_revenue')).show(5, truncate=False)

# Executor đã đăng ký với driver (mũi tên ⑤). Có cả driver trong danh sách.
status = sc._jsc.sc().getExecutorMemoryStatus()   # Map của Scala, không phải dict
hosts = sorted(status.keys().mkString(",").split(","))
print("block manager (driver + executor):", len(hosts))
for h in hosts:
    print("  ", h)

# --- 7. Ghi -----------------------------------------------------------------
# indirect: ghi tạm Parquet lên GCS rồi chạy 1 load job vào BigQuery (miễn
# phí). direct dùng Storage Write API, không cần bucket tạm nhưng tính phí.

if args.format == 'bigquery':
    (result.write.format('bigquery')
        .option('table', args.output)
        .option('parentProject', args.project)
        .option('writeMethod', 'indirect')
        .option('temporaryGcsBucket', args.temp_bucket)
        .mode('overwrite')
        .save())
else:
    result.write.mode('overwrite').parquet(args.output)
print("đã ghi:", args.output, f"({args.format})")

spark.stop()
