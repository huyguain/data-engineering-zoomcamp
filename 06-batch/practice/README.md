# Thực hành Spark local — module `06-batch`

Môi trường Spark local trên macOS, kèm script thực hành cho **Ngày 4** (bài 01 → 06:
đọc file, DataFrame, chuẩn hoá dữ liệu taxi) và **Ngày 5** (bài 07, 09, 10:
Spark SQL, group by, join).

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

# Bảng tra cứu vùng — Ngày 5 cần cho broadcast join. Repo đã có sẵn ở module dbt.
mkdir -p data/raw/zones
cp ../../04-analytics-engineering/taxi_rides_ny/seeds/taxi_zone_lookup.csv \
   data/raw/zones/
```

`download_taxi.sh` dùng `printf "%02d"` thay vì `{01..12}`: macOS ship bash 3.2,
đệm số 0 trong brace expansion chỉ có từ bash 4.0 nên `{01..12}` cho ra `1 2 3…`
và URL thành 404.

Cờ `-f` của curl khiến script dừng khi gặp tháng không tồn tại, thay vì lưu trang
lỗi 404 thành file `.csv.gz` 9 byte — loại file làm Spark chết với
`FAILED_READ_FILE` mà thông báo lỗi không hề nhắc tới chuyện file rỗng.

### Thư mục `2021/08` là rác

`download_taxi.sh` chạy `mkdir -p` **trước** `curl`, nên tháng 08 có thư mục nhưng
không có file (404). `03_prepare_taxi_data.py` kiểm tra bằng `os.path.isdir()` —
thư mục rỗng vẫn tính là tồn tại — nên nó vẫn ghi ra `data/pq/<loại>/2021/08/`, một
file parquet 12 KB **không có dòng nào**, chỉ có schema.

Hệ quả: `data/pq/green/2021/*` đọc ra 8 thư mục nhưng chỉ **7 tháng (01–07)** có dữ
liệu. Không sai kết quả, nhưng đừng nhầm là có dữ liệu tháng 8.

## Các script

| Script | Bài | Nội dung |
| --- | --- | --- |
| `01_first_look.py` | 04 | Đọc CSV, so sánh schema tự đoán và tự khai báo, đếm partition, ghi parquet |
| `02_dataframes.py` | 05 | Transformation vs action, `explain()`, `F.to_date` |
| `03_prepare_taxi_data.py` | 06 | Schema green (20 cột) và yellow (18 cột), vòng lặp CSV.gz → parquet |
| `04_checkpoint.py` | — | Checkpoint Ngày 4: đọc CSV hoặc parquet bất kỳ, in schema + 5 dòng |
| `05_spark_sql.py` | 07·09·10 | Ngày 5 từng bước: union green+yellow → temp view → group by → sort-merge join → broadcast join |
| `06_daily_zone_revenue.py` | — | Bài tập Ngày 5: job có tham số dòng lệnh, doanh thu theo ngày × vùng × loại |

```bash
source env.sh
python 01_first_look.py
python 04_checkpoint.py data/pq/green/2021/01/

# -i giữ lại REPL sau khi chạy xong, khỏi khởi động Spark lại (~10s mỗi lần)
python -i 05_spark_sql.py

python 06_daily_zone_revenue.py \
    --input_green  'data/pq/green/2021/*'  \
    --input_yellow 'data/pq/yellow/2021/*' \
    --zones        data/raw/zones/taxi_zone_lookup.csv \
    --output       data/report/daily_zone_revenue
```

## Kết quả đo được — Ngày 4

| Phép đo | Kết quả |
| --- | --- |
| `inferSchema` bật / tắt | 6,46s / 4,30s — quét toàn file chỉ để đoán kiểu |
| CSV thường / CSV.gz | 10 / 1 partition — gzip không chia nhỏ được |
| `select+filter` / `show()` | 0,038s / 0,67s — transformation không chạy gì |
| fhvhv: CSV → parquet | 717 MB → 216 MB |
| green: CSV.gz → parquet | 10 MB → 16 MB (parquet *to hơn*; lợi ích là schema và đọc theo cột) |

## Kết quả đo được — Ngày 5

Dữ liệu green + yellow 2021/01–07, múi giờ session mặc định `Asia/Ho_Chi_Minh`.

| Phép đo | Kết quả |
| --- | --- |
| green / yellow | 570.466 / 15.000.700 dòng — yellow gấp 26 lần |
| Cột chung trước / sau khi đổi tên `lpep_`·`tpep_` | 16 / 18 — thiếu đúng cột thời gian |
| Báo cáo doanh thu theo tháng | 3.713 dòng |
| Doanh thu giờ × zone (green / yellow) | 251.050 / 583.097 dòng |
| Dữ liệu phải shuffle (green) | 12,5 MiB · 327.000 dòng tạm — không phải 570.466 dòng gốc |
| Outer join hai bảng doanh thu | 669.590 dòng; chỉ 164.557 khoá có ở cả hai bên |
| `zones` | 265 dòng · 12.056 byte — dưới ngưỡng broadcast 10 MB |
| Doanh thu ngày × zone × loại | 91.049 dòng |

### AQE làm lệch bài học "200 partition" của video khoá học

Plan ghi `hashpartitioning(..., 200)` nhưng `getNumPartitions()` trả về **5** (green)
và **10** (yellow). Spark 4 bật sẵn Adaptive Query Execution: nó đo kích thước
shuffle thật (12,5 MiB), thấy chia 200 là phí, rồi **sửa plan ngay giữa lúc chạy**:

```
ShuffleQueryStage (7), Statistics(sizeInBytes=12.5 MiB, rowCount=3.27E+5)
AQEShuffleRead (8), coalesced
```

Video quay trên Spark 3.0 (AQE chưa mặc định) nên ra đúng 200 file nhỏ. Tắt đi để
thấy cảnh cũ: `spark.conf.set("spark.sql.adaptive.enabled", "false")`.

Lưu ý khi đọc plan: `explain()` chỉ biên dịch chứ không chạy, nên nó in ra plan
**trước** khi AQE can thiệp (`isFinalPlan=false`). Muốn xem plan thật thì chạy một
action trước rồi mới `explain()`, hoặc xem tab **SQL / DataFrame** trong Spark UI.

### `date_trunc` phụ thuộc múi giờ của máy chạy

Plan hiện `date_trunc(hour, ..., Some(Asia/Ho_Chi_Minh))` — Spark đang chia giờ theo
múi giờ Việt Nam cho dữ liệu New York. Cùng một dòng dữ liệu:

| `spark.sql.session.timeZone` | Đọc ra |
| --- | --- |
| `Asia/Ho_Chi_Minh` | `2009-01-01 00:03:25` |
| `UTC` (mặc định trên hầu hết cụm) | `2008-12-31 17:03:25` |

Lệch 7 tiếng, và đổi cả ngày. Job doanh thu theo ngày cho ra 91.049 dòng ở giờ Việt
Nam nhưng **91.280** ở `America/New_York`. Luôn set tường minh:

```python
spark.conf.set("spark.sql.session.timeZone", "America/New_York")
```

`06_daily_zone_revenue.py` có cờ `--timezone` để so sánh hai cách.

### Dữ liệu taxi bẩn hơn nhiều so với vẻ ngoài

Trên 15.000.700 dòng yellow:

| Bất thường | Số chuyến |
| --- | --- |
| `total_amount` âm | 68.820 |
| `total_amount` = 0 | 4.042 |
| `total_amount` > 10.000 USD | 2 |
| `trip_distance` = 0 dặm | 199.040 |

Biên độ: `max = 398.469,20` · `min = −647,80` · trung bình `18,76`.

Chuyến đắt nhất — **0 dặm, 398.469,20 USD** — nằm ở zone 161 ngày 18/03/2021, và nó
leo thẳng lên dòng đầu báo cáo:

| Midtown Center, 18/03 | Có dòng rác | Bỏ dòng rác |
| --- | --- | --- |
| Tổng doanh thu | 442.708,07 | 44.238,87 |
| Số chuyến | 2.803 | 2.802 |
| Trung bình mỗi chuyến | 157,94 | 15,79 |

Một dòng trên 2.803 dòng chiếm 90% doanh thu cả ngày. Job chạy đúng về kỹ thuật
nhưng sai về nghiệp vụ, và không có cảnh báo nào. Pipeline thật cần tầng kiểm tra
chất lượng trước khi aggregate.

### Ba chiến lược join

| Chiến lược | Spark chọn khi | Chi phí mạng | Dấu hiệu trong plan |
| --- | --- | --- | --- |
| Broadcast hash join | Một bên < `autoBroadcastJoinThreshold` (10 MB) | Gửi bảng nhỏ, 1 lần | `BroadcastExchange` + `BroadcastHashJoin` |
| Sort-merge join | Mặc định cho hai bảng lớn | Xáo **cả hai** bên | 2× `Exchange` + 2× `Sort` + `SortMergeJoin` |
| Shuffle hash join | Một bên vừa RAM executor sau khi xáo | Xáo cả hai, không sort | `ShuffledHashJoin`, không có `Sort` |

### Hai cái bẫy đã gặp

**Cột trùng tên.** Bảng `zones` có cột `zone` (tên vùng, chuỗi) trùng với cột `zone`
(mã vùng, số) của bảng doanh thu. Không đổi tên trước khi join thì `drop('zone')`
xoá **cả hai**, và mọi tham chiếu sau đó báo `AMBIGUOUS_REFERENCE`. Xử lý trùng tên
trước khi join, đừng dọn sau.

**Thứ tự cột khi union.** `unionAll` ghép theo **vị trí** chứ không theo tên. Lấy tập
cột chung bằng `set(a) & set(b)` cho ra thứ tự ngẫu nhiên theo hash, hai DataFrame có
thể lệch nhau, và Spark **không báo lỗi** vì các cột đều là `double`. Duyệt theo thứ
tự cột của một bên (`[c for c in df_green.columns if c in yellow_cols]`), hoặc dùng
`unionByName`.

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
