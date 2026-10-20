"""Checkpoint ngày 4.

"Viết được script Spark đọc file CSV/Parquet taxi và in ra schema + vài dòng."

Chạy:
    python 04_checkpoint.py data/raw/green/2021/01/green_tripdata_2021_01.csv.gz
    python 04_checkpoint.py data/pq/green/2021/01/
"""

import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession


def read_any(spark, path):
    """Đoán định dạng theo đuôi file rồi đọc.

    CSV không mang schema nên phải bật inferSchema - Spark quét dữ liệu để đoán
    kiểu. Parquet mang sẵn schema trong file nên không cần gì cả.
    """
    if path.rstrip("/").endswith((".csv", ".csv.gz")) or "/raw/" in path:
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )
    return spark.read.parquet(path)


def main(path):
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("checkpoint-day4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = read_any(spark, path)

    print(f"\nNguồn       : {path}")
    print(f"Số partition: {df.rdd.getNumPartitions()}")
    print(f"Số dòng     : {df.count():,}")
    print(f"Số cột      : {len(df.columns)}")
    print("\nSchema:")
    df.printSchema()
    print("5 dòng đầu:")
    df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Dùng: python 04_checkpoint.py <đường dẫn CSV hoặc thư mục parquet>")
    main(sys.argv[1])
