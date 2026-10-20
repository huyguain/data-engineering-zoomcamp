from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
import time

# Create SparkSession
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("First Look")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version)
print("Số core Spark thấy:", spark.sparkContext.defaultParallelism)

# Read CSV file
CSV = "data/raw/fhvhv_tripdata_2021-01.csv"
df = spark.read.option("header", "true").csv(CSV)

df.printSchema()
df.show(5, truncate=False)

# Check the schema of the CSV file using pandas
df_pandas = pd.read_csv("data/raw/head.csv")
print("Dtypes from pandas:", df_pandas.dtypes)
print("Schema from Spark:", spark.createDataFrame(df_pandas).schema)

schema = types.StructType([
    types.StructField("hvfhs_license_num", types.StringType(), True),
    types.StructField("dispatching_base_num", types.StringType(), True),
    types.StructField("pickup_datetime", types.TimestampType(), True),
    types.StructField("dropoff_datetime", types.TimestampType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("SR_Flag", types.StringType(), True),
])
df = spark.read.option("header", "true").schema(schema).csv(CSV)
df.printSchema()
df.show(5, truncate=False)

print("CSV thuong:", df.rdd.getNumPartitions())
df_gz = spark.read.option("header", "true").schema(schema).csv("data/raw/fhvhv_tripdata_2021-01.csv.gz")
print("CSV nén:", df_gz.rdd.getNumPartitions())

# Repartition the DataFrame to 24 partitions
df = df.repartition(24)

t0 = time.time()
df.write.mode("overwrite").parquet("data/pq/2021/01")
print("Thời gian ghi Parquet:", time.time() - t0)

input("Mở http://localhost:4040/ để xem Spark UI. Nhấn Enter để tiếp tục...")

spark.stop()