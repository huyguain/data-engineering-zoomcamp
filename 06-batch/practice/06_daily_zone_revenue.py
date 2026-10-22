"""Doanh thu theo ngày × vùng × loại taxi.

Bài tập buổi chiều Ngày 5: join 2 bảng taxi, groupBy theo vùng/ngày,
tính count + sum + avg, rồi gắn tên vùng bằng broadcast join.

Chạy:
  source env.sh
  python 06_daily_zone_revenue.py \
      --input_green  'data/pq/green/2021/*'  \
      --input_yellow 'data/pq/yellow/2021/*' \
      --zones        data/raw/zones/taxi_zone_lookup.csv \
      --output       data/report/daily_zone_revenue

Thêm --timezone America/New_York để chia ngày theo giờ New York thay vì
giờ máy. Xem phần "Múi giờ" trong README.
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
parser.add_argument('--date_from',    default='2021-01-01')
parser.add_argument('--date_to',      default='2021-09-01')   # loại trừ
parser.add_argument('--timezone',     default=None)
args = parser.parse_args()

# --- 2. Session -----------------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("daily-zone-revenue")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Không set thì date_trunc chia ngày theo múi giờ của MÁY đang chạy - cùng
# một job cho kết quả khác nhau trên laptop và trên cụm.
if args.timezone:
    spark.conf.set("spark.sql.session.timeZone", args.timezone)
print("múi giờ session:", spark.conf.get("spark.sql.session.timeZone"))

# --- 3. Đọc và chuẩn hoá tên cột ------------------------------------------
# green dùng tiền tố lpep_ (livery), yellow dùng tpep_ (taxi). Phải đổi về
# cùng tên TRƯỚC khi lấy tập cột chung, nếu không cột thời gian biến mất.

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

# --- 4. Tập cột chung -----------------------------------------------------
# unionAll ghép theo VỊ TRÍ cột chứ không theo tên, nên thứ tự phải giống
# nhau ở cả hai bên. set() không có thứ tự -> duyệt theo thứ tự của green.

yellow_cols = set(df_yellow.columns)
common = [c for c in df_green.columns if c in yellow_cols]

# --- 5. Gộp hai bảng ------------------------------------------------------

trips = (
    df_green.select(common).withColumn('service_type', F.lit('green'))
    .unionAll(
    df_yellow.select(common).withColumn('service_type', F.lit('yellow')))
)
trips.createOrReplaceTempView('trips_data')

# --- 6. Gom theo ngày × vùng × loại ---------------------------------------
# Hai điều kiện >= và < (không dùng BETWEEN - BETWEEN tính cả cận trên).
# Cả hai đều xuống được PushedFilters nên Spark bỏ qua row group không cần.

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

# --- 7. Gắn tên vùng bằng broadcast join ----------------------------------
# Bảng zones có cột tên 'zone' trùng với khái niệm zone_id -> đổi tên trước
# khi join, nếu không drop('zone') sẽ xoá cả hai cột cùng tên.

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
)

# --- 8. Kiểm tra và ghi ---------------------------------------------------

print("số dòng:", result.count())
result.orderBy(F.desc('total_revenue')).show(5, truncate=False)
result.explain(mode="formatted")

result.write.mode('overwrite').parquet(args.output)
print("đã ghi:", args.output)

spark.stop()
