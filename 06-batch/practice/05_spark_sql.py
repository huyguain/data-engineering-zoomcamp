import os, sys
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("day-05-spark-sql")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df_green = spark.read.parquet("data/pq/green/2021/*")
df_yellow = spark.read.parquet("data/pq/yellow/2021/*")

print("green: ", df_green.count(), df_green.rdd.getNumPartitions(), "partitions")
print("yellow: ", df_yellow.count(), df_yellow.rdd.getNumPartitions(), "partitions")

 # --- Bước 2: chuẩn hoá tên cột rồi lấy tập cột chung ---------------------
 
df_green = (
    df_green
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
)

df_yellow = (
    df_yellow
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
)

# giữ ĐÚNG thứ tự cột của green — không dùng set(), set không có thứ tự
yellow_cols = set(df_yellow.columns)
common = [
    c for c in df_green.columns if c in yellow_cols
]

print("common columns: ", len(common))
print(common)

# --- Bước 3: gộp green + yellow thành một bảng trips_data ----------------
df_green_sel = df_green.select(common).withColumn("service_type", F.lit("green"))
df_yellow_sel = df_yellow.select(common).withColumn("service_type", F.lit("yellow"))

df_trips = df_green_sel.unionByName(df_yellow_sel)
df_trips.createOrReplaceTempView("trips_data")

spark.sql("SELECT service_type, COUNT(1) AS trips FROM trips_data GROUP BY service_type").show()

# --- Bước 4: GROUP BY theo giờ × zone -----------------------------------
df_green.createOrReplaceTempView("green")
df_yellow.createOrReplaceTempView("yellow")

def revenue(table):
    return spark.sql(f"""
        SELECT
            DATE_TRUNC('hour', pickup_datetime) AS hour,
            PULocationID AS zone,
            SUM(total_amount) AS revenue,
            COUNT(1) AS number_records
        FROM {table}
        WHERE pickup_datetime >= '2021-01-01'
        GROUP BY 1, 2
    """)

df_green_rev = revenue("green")
df_yellow_rev = revenue("yellow")

print("green revenue: ", df_green_rev.count(), df_green_rev.rdd.getNumPartitions(), "partitions")
print("yellow revenue: ", df_yellow_rev.count(), df_yellow_rev.rdd.getNumPartitions(), "partitions")

# df_green_rev.explain(mode="formatted")

# --- Bước 5: gộp revenue green + yellow thành một bảng revenue_data ----------------
g = (
    df_green_rev
    .withColumnRenamed("revenue", "green_revenue")
    .withColumnRenamed("number_records", "green_trips")
)

y = (
    df_yellow_rev
    .withColumnRenamed("revenue", "yellow_revenue")
    .withColumnRenamed("number_records", "yellow_trips")
)

df_join = g.join(y, ["hour", "zone"], "outer")

print("join:", df_join.count(), "dòng")
df_join.orderBy("hour", "zone").show(5, truncate=False)
# df_join.explain(mode="formatted")

# --- Bước 6: join với bảng tra cứu zones (broadcast) --------------------
df_zones = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/raw/zones/taxi_zone_lookup.csv")
    .withColumnRenamed("zone", "zone_name")
)

print("zones: ", df_zones.count(), "rows")
df_with_zones = (
    df_join
    .join(df_zones, df_join.zone == df_zones.locationid)
    .drop("locationid")
)

print("sau join:", df_with_zones.count(), "rows")
df_with_zones.orderBy("hour", "zone").show(5, truncate=False)
df_with_zones.explain(mode="formatted")

print("ngưỡng broadcast: ", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
print("zones nặng:", df_zones._jdf.queryExecution().optimizedPlan().stats().sizeInBytes(), "byte")