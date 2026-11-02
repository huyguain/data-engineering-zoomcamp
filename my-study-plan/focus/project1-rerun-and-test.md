# Project 1 — Chạy lại từ đầu và tự test để hiểu bản chất

Mỗi phần có 3 bước: **Chạy** (lệnh), **Bản chất** (điều gì xảy ra bên dưới), **Test** (dự đoán trước kết quả, rồi chạy để kiểm chứng). Nếu dự đoán sai thì đó chính là chỗ cần ôn lại.

```
CSV.gz (TLC) ─► GCS ─► BigQuery external ─► native partitioned / clustered      (Phần 1)
                │
                └─► DuckDB ─► dbt: staging → intermediate → mart → reporting      (Phần 2)

CSV.gz ─► Parquet ─► Spark local ─► Spark trên YARN ─► đọc GCS, ghi BigQuery      (Phần 3–5)
```

Số đo thật dùng để tự kiểm tra kết quả (đã chạy lại ngày 29/10/2026):

| Kiểm tra | Giá trị đúng |
|---|---|
| Dry run, 1 tuần, bảng non-partitioned / partitioned | 235,935,792 / 29,280,768 byte |
| Spark: số chuyến trong khoảng 2021-01-01 → 2021-09-01 | 15,562,140 (trên tổng 15,571,166 dòng nguồn) |
| Tổng doanh thu output, cũng là bảng BigQuery `daily_zone_revenue_yarn` | 294,750,068.34 USD, 91,280 dòng |
| dbt build | 9 model, 42 test, 2 seed, tất cả pass |

---

## Phần 0 — Chuẩn bị (một lần)

```bash
gcloud auth login                         # cho lệnh CLI (bq, gcloud)
gcloud auth application-default login     # ADC, cho THƯ VIỆN (Spark connector) dùng
gcloud config set project de-zoomcamp-500308
```

**Bản chất:** có 2 loại credential khác nhau. `auth login` chỉ dành cho CLI. Code chạy trong JVM, như GCS connector và BigQuery connector, tìm credential theo chuỗi ADC. Thiếu bước thứ hai thì Spark báo `Your default credentials were not found`.

---

## Phần 1 — BigQuery: partition và cluster

### Chạy

Bảng đã tạo sẵn theo [big_query.sql](../../03-data-warehouse/big_query.sql). Nếu muốn tạo lại thì chạy 3 câu `CREATE` trong file đó, đổi `taxi-rides-ny` thành `de-zoomcamp-500308`:

```sql
CREATE OR REPLACE TABLE nytaxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM nytaxi.external_yellow_tripdata;
```

Kiểm tra cấu trúc:

```bash
bq ls de-zoomcamp-500308:nytaxi                       # cột Time Partitioning / Clustered Fields
bq show --format=prettyjson de-zoomcamp-500308:nytaxi.yellow_tripdata_partitioned_clustered \
  | grep -A3 -E 'timePartitioning|clustering'
```

### Bản chất

- **Lưu trữ theo cột.** BigQuery tính tiền theo **số byte của các cột bị đọc** × **các partition không bị loại**. Số dòng trả về không ảnh hưởng.
- **Partition** chia bảng thành các phần vật lý riêng, mỗi ngày một phần. Nếu `WHERE` trên cột partition được tính ra **trước khi chạy**, BigQuery bỏ hẳn các partition không liên quan. Đó là lý do **dry run thấy được** tác dụng của partition.
- **Cluster** sắp xếp dữ liệu *bên trong* mỗi partition theo `VendorID`, rồi chia thành block. Mỗi block có metadata min/max. Việc bỏ block chỉ xảy ra **lúc chạy thật**, nên dry run **không** thấy tác dụng của cluster. Phải xem "Bytes billed" sau khi chạy.

### Test (dự đoán trước, rồi chạy)

Đặt biến cho gọn: `P=de-zoomcamp-500308.nytaxi`

```bash
dry() { bq query --use_legacy_sql=false --dry_run "$1" 2>&1 | tail -1; }
```

| # | Query (`dry "..."`) | Dự đoán của bạn | Đáp án |
|---|---|---|---|
| 1 | `SELECT DISTINCT VendorID FROM \`$P.yellow_tripdata_non_partitioned\` WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-02-01' AND '2019-02-07'` | ? | 235.9 MB: không có partition nên đọc cả cột |
| 2 | Như câu 1, nhưng trên bảng `yellow_tripdata_partitioned` | ? | 29.3 MB: chỉ đọc 7 trên ~59 ngày |
| 3 | `SELECT DISTINCT VendorID FROM \`$P.yellow_tripdata_partitioned\` WHERE EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) = 1` | ? | **235.9 MB**: điều kiện "Chủ nhật" không quy được về khoảng ngày, nên **không** loại được partition nào |
| 4 | `... WHERE tpep_pickup_datetime BETWEEN '2019-02-01' AND '2019-02-07 23:59:59'` (lọc thẳng timestamp, không dùng `DATE()`) | ? | 29.3 MB: BigQuery vẫn tự quy ra khoảng partition |
| 5 | `SELECT * FROM \`$P.yellow_tripdata_non_partitioned\` LIMIT 10` | ? | ~2 GB: `LIMIT` không giảm chi phí |
| 6 | `SELECT VendorID FROM \`$P.yellow_tripdata_non_partitioned\` LIMIT 10` | ? | Chỉ đọc 1 cột. Đây mới là cách giảm chi phí |

**Test cluster** (chạy thật, mỗi câu khoảng 200 MB, nằm trong free tier):

```bash
for t in yellow_tripdata_partitioned yellow_tripdata_partitioned_clustered; do
  bq query --use_legacy_sql=false --nouse_cache --format=none \
    "SELECT COUNT(*) FROM \`$P.$t\` WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-02-28' AND VendorID = 1"
  bq ls -j -n 1 --format=prettyjson | grep -E '"totalBytesBilled"'
done
```
Kết quả: 235.9 MB so với 184.6 MB (giảm ~22%). **Phải có `--nouse_cache`**, nếu không câu thứ hai lấy kết quả từ cache và báo 0 byte.

**Test dữ liệu bẩn:**

```sql
SELECT partition_id, total_rows
FROM `de-zoomcamp-500308.nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY partition_id;
```

Bạn sẽ thấy các partition từ năm 2001 tới 2088. Đó là các chuyến có giờ sai. **Câu nói khi phỏng vấn:** *"partition cũng là một cách soi chất lượng dữ liệu; tôi sẽ thêm test khoảng ngày ở tầng staging."*

---

## Phần 2 — dbt: staging → mart, có test

### Chạy

Máy hiện **chưa có lệnh `dbt`** trong PATH, nên phải cài trước:

```bash
cd 04-analytics-engineering/taxi_rides_ny
python3.13 -m venv .venv && source .venv/bin/activate
pip install dbt-duckdb                    # kèm theo duckdb
dbt --version                             # đọc ~/.dbt/profiles.yml: type duckdb, target dev
dbt debug                                 # kiểm tra kết nối
dbt deps                                  # cài dbt_utils (dùng cho unique_combination_of_columns)
# dữ liệu yellow/green 2019/01–02 đã có trong data/; thiếu thì chạy: python load_data.py
dbt build                                 # seed + run + test theo thứ tự DAG
```

### Bản chất

- `source()` và `ref()` không chỉ là tên bảng. dbt đọc chúng để **dựng DAG**, từ đó biết model nào chạy trước.
- `dbt build` đi theo DAG: **seed → model → test của model đó → model phía sau**. Test của một model fail thì các model phía sau bị **SKIP**, nên dữ liệu lỗi không lan xuống.
- **Một test chỉ là một câu `SELECT` trả về các dòng vi phạm.** Trả về 0 dòng thì pass.

### Test

1. **Xem DAG bằng chữ:**
   ```bash
   dbt ls --select +fct_daily_zone_revenue    # mọi model phía trên
   dbt ls --select stg_green_tripdata+         # mọi model phía dưới
   ```
2. **Mở ruột một test:**
   ```bash
   dbt compile --select fct_daily_zone_revenue
   ls target/compiled/taxi_rides_ny/models/marts/reporting/schema.yml/
   cat target/compiled/taxi_rides_ny/models/marts/reporting/schema.yml/dbt_utils_unique_combination*.sql
   ```
   Bạn sẽ thấy một câu `GROUP BY pickup_zone, revenue_day, service_type HAVING COUNT(*) > 1`. Đây chính là định nghĩa grain viết bằng SQL.
3. **Làm hỏng có chủ đích để thấy SKIP:** trong [intermediate/schema.yml](../../04-analytics-engineering/taxi_rides_ny/models/intermediate/schema.yml), ở model `int_trips`, cột `service_type`, đổi `values: ['Green', 'Yellow']` thành `['Yellow']`. Sau đó:
   ```bash
   dbt build --select int_trips+ --store-failures
   ```
   Dự đoán: test FAIL, các model phía sau ghi `SKIP`. `--store-failures` lưu các dòng lỗi vào một bảng để query xem. **Nhớ hoàn tác file sau khi thử.**
4. **Tự kiểm tra grain bằng tay**, không dùng dbt:
   ```bash
   python -c "import duckdb; c=duckdb.connect('taxi_rides_ny.duckdb', read_only=True); print(c.sql('''
     SELECT COUNT(*) n, COUNT(DISTINCT (pickup_zone, revenue_day, service_type)) k
     FROM dev.fct_daily_zone_revenue'''))"
   ```
   Dự đoán: `n = k`. Nếu `n > k` thì grain đang bị nhân dòng.
5. **Lineage:** chạy `dbt docs generate && dbt docs serve`, rồi tự chỉ ra từng layer trên đồ thị.

**Lưu ý so với CV:** dbt đang chạy trên **DuckDB**, trong khi CV ghi "on top of a BigQuery warehouse". Hoặc thêm target `bigquery` vào `profiles.yml` rồi chạy `dbt build --target bigquery` một lần, hoặc sửa CV.

---

## Phần 3 — Spark local: job doanh thu và cách đo

### Chạy

```bash
cd 06-batch/practice && source env.sh
python 06_daily_zone_revenue.py \
  --input_green 'data/pq/green/2021/*' --input_yellow 'data/pq/yellow/2021/*' \
  --zones data/raw/zones/taxi_zone_lookup.csv \
  --output data/report/daily_zone_revenue --timezone America/New_York

python 13_reconcile.py data/report/daily_zone_revenue    # test đối chiếu
python 11_stage_probe.py                                 # đo stage và shuffle qua REST API
```

### Bản chất

- **Lazy evaluation:** `read`, `select`, `groupBy`, `join` chỉ dựng kế hoạch. Chỉ có **action** (`count`, `show`, `write`) mới chạy job.
- **Catalyst** tối ưu kế hoạch: chỉ đọc các cột cần dùng (column pruning) và đẩy điều kiện lọc xuống tầng đọc Parquet (predicate pushdown).
- **Job bị cắt thành stage tại `Exchange`**, tức chỗ shuffle. `groupBy` cần gom các dòng cùng key về một chỗ nên phải shuffle. Trước khi shuffle, mỗi partition **tự gộp trước** (`partial_sum`), nên 15,5 triệu dòng chỉ còn 6,5 MB phải gửi đi.
- **Broadcast join:** bảng zone (265 dòng) được gửi tới mọi executor, nên bảng lớn không phải shuffle.

### Test

1. **Đối chiếu bằng đường tính khác** (`13_reconcile.py`): tính lại tổng số chuyến và doanh thu trực tiếp từ nguồn, không qua `groupBy` hay `join`.
   - Kết quả đúng: `15,562,140` chuyến, `294,750,068.34` USD, `0` dòng trùng grain.
   - **Vì sao test này quan trọng:** job dùng **inner join** với bảng zone. Nếu có `PULocationID` không nằm trong bảng zone thì chuyến đó **âm thầm biến mất**. Nếu bảng zone bị trùng key thì doanh thu **bị nhân đôi**. Tổng số chuyến khớp chứng minh cả hai lỗi đều không xảy ra.
   - Câu hỏi: 15,571,166 − 15,562,140 = 9,026 chuyến đi đâu? Trả lời: đó là các chuyến có ngày nằm ngoài 2021-01 → 2021-08 (dữ liệu bẩn), bị điều kiện `WHERE` loại ra.
2. **Xem kế hoạch thực thi:** chạy `python -i 05_spark_sql.py`, rồi trong REPL gõ `df.explain("formatted")` cho một DataFrame có `groupBy`. Tìm `HashAggregate (partial)` → `Exchange hashpartitioning` → `HashAggregate (final)`.
3. **Tắt broadcast để thấy sự khác biệt:** `11_stage_probe.py` chạy 3 cấu hình (broadcast + AQE, sort-merge + AQE, sort-merge không AQE). Dự đoán: khi không broadcast, xuất hiện `SortMergeJoin` và thêm một lượt shuffle phía bảng lớn.
4. **Spark UI:** thêm `input("Mở http://localhost:4040 rồi Enter")` trước `spark.stop()`. Ở tab **Stages**, so sánh thời gian task max với median. Chênh lệch lớn tức là có skew.

---

## Phần 4 — Spark trên YARN (1 node)

### Chạy

```bash
cd 06-batch/practice && source env-yarn.sh        # thêm HADOOP_CONF_DIR, YARN_CONF_DIR
yarn --daemon start resourcemanager && yarn --daemon start nodemanager
yarn node -list                                    # 1 node RUNNING; UI tại http://localhost:8088

spark-submit --master yarn --deploy-mode client \
  --num-executors 2 --executor-cores 2 --executor-memory 2g \
  10_revenue_cloud.py \
    --input_green "file://$PWD/data/pq/green/2021/*" --input_yellow "file://$PWD/data/pq/yellow/2021/*" \
    --zones "file://$PWD/data/raw/zones/taxi_zone_lookup.csv" --timezone America/New_York \
    --output "file://$PWD/data/report/yarn_local"

python 13_reconcile.py data/report/yarn_local      # phải ra đúng số như chạy local
yarn application -list -appStates FINISHED | tail -3
```

**Phải ghi `file://`** vì `env-yarn.sh` nạp `core-site.xml` có `fs.defaultFS=hdfs://`. Không có scheme thì đường dẫn bị hiểu là đường dẫn trên HDFS.

### Bản chất

| Thành phần | Vai trò | Trong lab |
|---|---|---|
| ResourceManager | Quản lý tài nguyên toàn cụm, cấp container | 1 |
| NodeManager | Chạy container trên mỗi máy, báo tài nguyên về RM | 1, cấu hình 8 GB / 6 vcore |
| ApplicationMaster | Mỗi app có một AM, đi xin RM cấp container cho executor | 1 container 1 GB |
| Executor | JVM chạy task, nằm trong container | 2 × 3 GB |
| Driver | Ở chế độ `client`, nằm trên máy gõ lệnh | máy Mac |

**Phép tính bộ nhớ:** `2048 + max(384, 10%) = 2432 MB`, làm tròn lên bội số `minimum-allocation-mb` (1024) thành **3072 MB**. Tổng 2 executor × 3 GB + AM 1 GB = 7 GB, vừa trong 8 GB.

### Test

1. **Dự đoán rồi thử `--num-executors 3`:** cần 3 × 3 + 1 = 10 GB > 8 GB. YARN chỉ cấp được 2 executor, executor thứ 3 nằm chờ. Xem trên :8088 hoặc Spark UI tab **Executors**.
2. **Bẫy `.master("local[*]")`:** submit `06_daily_zone_revenue.py` (có `.master` cứng trong code) với `--master yarn`. Dự đoán: job vẫn chạy xong nhưng **không xuất hiện** trên :8088, vì cấu hình trong code thắng cấu hình dòng lệnh. `10_revenue_cloud.py` in ra `master :` ở đầu để tự kiểm tra.
3. **Vì sao :8088 hiện `vCores:1` dù đã xin 2 core?** Vì `DefaultResourceCalculator` chỉ tính RAM. Spark vẫn chạy 2 task song song trong mỗi executor.
4. **Đọc log của một container:** `yarn logs -applicationId <id> | less`. Tìm dòng executor đăng ký với driver.

---

## Phần 5 — Đọc GCS, ghi BigQuery

### Chạy

```bash
B=gs://de-zoomcamp-500308-nytaxi
gcloud storage ls $B/pq/yellow/2021/ | head         # dữ liệu đã có trên bucket
spark-submit --master yarn --deploy-mode client \
  --num-executors 2 --executor-cores 2 --executor-memory 2g \
  --jars lib/gcs-connector-3.1.9.jar \
  --packages com.google.cloud.spark:spark-4.0-bigquery:0.46.0 \
  10_revenue_cloud.py \
    --input_green "$B/pq/green/2021/*" --input_yellow "$B/pq/yellow/2021/*" \
    --zones "$B/zones/taxi_zone_lookup.csv" --timezone America/New_York \
    --format bigquery --output nytaxi.daily_zone_revenue_yarn
```

Mất khoảng 5 phút vì đọc qua mạng nhà.

### Bản chất

- **`gs://` là một Hadoop FileSystem.** GCS connector cài đặt cùng interface với `hdfs://`, nên code Spark không phải đổi gì, chỉ đổi đường dẫn. Đổi lại là **mất data locality**: dữ liệu luôn phải đi qua mạng.
- **Ghi `indirect`:** Spark ghi Parquet tạm lên bucket, rồi chạy **1 load job** vào BigQuery (load job miễn phí), sau đó xóa file tạm. Cách `direct` dùng Storage Write API, không cần bucket tạm nhưng tốn phí.

### Test

```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n, ROUND(SUM(total_revenue),2) rev, COUNT(DISTINCT zone_id) zones
   FROM nytaxi.daily_zone_revenue_yarn"
```
Kết quả đúng: `91280`, `294750068.34`, `263`. Doanh thu **khớp tới cent** với `13_reconcile.py` chạy local, chứng minh 3 môi trường (local, YARN đọc file local, YARN đọc GCS) cho cùng kết quả.

**Câu hỏi tự trả lời:**
- Vì sao chạy trên YARN đọc GCS mất 4 phút 54 giây, còn chạy local chỉ 15,8 giây? Có 2 lý do: đọc qua mạng, và 3 action (`count`, `show`, `write`) không có `cache()` nên 428 MB bị đọc lại 3 lần.
- Vì sao trong BigQuery cột `day` hiện `05:00:00`? Vì 0 giờ ở New York là 5 giờ UTC, và kiểu `TIMESTAMP` luôn lưu theo UTC. Nên ghi cột kiểu `DATE`.

---

## Checklist "đã hiểu bản chất" (nói to, không nhìn tài liệu)

- [ ] Vì sao câu 3 (Chủ nhật) quét toàn bảng dù có `WHERE` trên cột partition?
- [ ] Vì sao dry run thấy tác dụng của partition mà không thấy tác dụng của cluster?
- [ ] Test `unique_combination_of_columns` thực chất là câu SQL nào?
- [ ] Test fail thì model phía sau bị gì? Vì sao đó là điều tốt?
- [ ] Inner join với dim có thể làm sai số liệu theo 2 cách nào? Test nào bắt được?
- [ ] Trong job Spark, stage bị cắt ở đâu? Vì sao shuffle chỉ còn 6,5 MB?
- [ ] Vì sao xin 2g mà YARN cấp container 3 GB? Xin 3 executor thì sao?
- [ ] Vì sao `.master("local[*]")` trong code làm job chạy local mà không báo lỗi gì?
- [ ] `gs://` và `hdfs://` giống và khác nhau ở đâu?
