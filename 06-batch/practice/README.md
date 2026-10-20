# Ngày 4 — Spark nhập môn (thực hành)

Môi trường Spark local trên macOS cho module `06-batch`, kèm các script thực hành
bài 01 → 06.

## Cài một lần

```bash
brew install openjdk@17                       # Spark 4.x cần Java 17
python3.13 -m venv .venv
.venv/bin/pip install pyspark "pandas<3" pyarrow
```

PySpark 4.x đã gói sẵn Spark bên trong — không cần tải Spark riêng, và **không
được** set `SPARK_HOME` trỏ sang bản Spark 3.x cũ (PySpark sẽ nạp nhầm JAR rồi hỏng).

Phiên bản đã kiểm chứng: OpenJDK 17.0.20 · Python 3.13.16 · PySpark 4.2.0 · pandas 2.3.3.

## Mỗi lần mở terminal

```bash
source env.sh
```

| Biến trong `env.sh` | Không có nó thì sao |
| --- | --- |
| `JAVA_HOME` | Homebrew cài openjdk dạng keg-only nên `java` không vào PATH — Spark chết với `JAVA_GATEWAY_EXITED` |
| `PYSPARK_PYTHON` | Python mà *executor* dùng để chạy UDF. Không set thì Spark gọi python 3.9 của macOS (không có pyspark) và UDF chết giữa chừng |
| `PYSPARK_DRIVER_PYTHON` | Python của driver — lệch phiên bản với executor là Spark từ chối chạy |

`JAVA_HOME` trong `env.sh` là đường dẫn Homebrew trên Apple Silicon
(`/opt/homebrew/opt/openjdk@17`). Máy Intel hoặc Linux cần sửa lại dòng này.

## Tải dữ liệu

```bash
# FHVHV tháng 1/2021 — 11.908.468 dòng, 717 MB khi giải nén
mkdir -p data/raw
curl -L -o data/raw/fhvhv_tripdata_2021-01.csv.gz \
  https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz
gunzip -kc data/raw/fhvhv_tripdata_2021-01.csv.gz > data/raw/fhvhv_tripdata_2021-01.csv
head -n 101 data/raw/fhvhv_tripdata_2021-01.csv > data/raw/head.csv

# Yellow và green 2021 — cả hai chỉ có dữ liệu tới tháng 7
bash download_taxi.sh green 2021
bash download_taxi.sh yellow 2021
```

`download_taxi.sh` dùng `printf "%02d"` thay vì `{01..12}`: macOS ship bash 3.2,
đệm số 0 trong brace expansion chỉ có từ bash 4.0 nên `{01..12}` cho ra `1 2 3…`
và URL thành 404.

Cờ `-f` của curl khiến script dừng khi gặp tháng không tồn tại, thay vì lưu trang
lỗi 404 thành file `.csv.gz` 9 byte — loại file làm Spark chết với
`FAILED_READ_FILE` mà thông báo lỗi không hề nhắc tới chuyện file rỗng.

## Các script

| Script | Bài | Nội dung |
| --- | --- | --- |
| `01_first_look.py` | 04 | Đọc CSV, so sánh schema tự đoán và tự khai báo, đếm partition, ghi parquet |
| `02_dataframes.py` | 05 | Transformation vs action, `explain()`, `F.to_date` |
| `03_prepare_taxi_data.py` | 06 | Schema green (20 cột) và yellow (18 cột), vòng lặp CSV.gz → parquet |
| `04_checkpoint.py` | — | Checkpoint: đọc CSV hoặc parquet bất kỳ, in schema + 5 dòng |

```bash
source env.sh
python 01_first_look.py
python 04_checkpoint.py data/pq/green/2021/01/
```

## Kết quả đo được

| Phép đo | Kết quả |
| --- | --- |
| `inferSchema` bật / tắt | 6,46s / 4,30s — quét toàn file chỉ để đoán kiểu |
| CSV thường / CSV.gz | 10 / 1 partition — gzip không chia nhỏ được |
| `select+filter` / `show()` | 0,038s / 0,67s — transformation không chạy gì |
| fhvhv: CSV → parquet | 717 MB → 216 MB |
| green: CSV.gz → parquet | 10 MB → 16 MB (parquet *to hơn*; lợi ích là schema và đọc theo cột) |

## Xem Spark UI

`http://localhost:4040`, chỉ sống khi SparkSession còn chạy. Muốn kịp xem, thêm
trước `spark.stop()`:

```python
input("Mở http://localhost:4040 rồi Enter để thoát...")
```

Lưu ý với Spark 4: job ghi parquet hiện tên `$anonfun$withThreadLocalCaptured$1`
trong tab Jobs, không phải `parquet` như trong video của khoá. Chữ `parquet` nằm ở
tab **SQL / DataFrame** — cũng là nơi duy nhất nhìn thấy ô `Exchange` của shuffle.

## Lưu ý

Thư mục `data/` không được commit (xem `.gitignore`) — mọi thứ trong đó tải lại
bằng `download_taxi.sh` hoặc sinh lại bằng `03_prepare_taxi_data.py`.
