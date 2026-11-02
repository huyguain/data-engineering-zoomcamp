# Ngày 2 — Thực hành Spark + Hadoop

Dữ liệu: green + yellow 2021/01–07 dạng Parquet trong `06-batch/practice/data/pq/`. Tổng cộng **15,571,166 dòng** (green 570,466 + yellow 15,000,700).
Môi trường: PySpark 4.2.0, Java 17, `local[*]` với 10 core.

```bash
cd 06-batch/practice && source env.sh
```

---

## Tình trạng so với CV (kiểm tra ngày 27/10/2026)

| Dòng trong CV | Bằng chứng trên máy | Trạng thái |
|---|---|---|
| Spark DataFrame + RDD, join/group/aggregate | `05`–`09`, `06_daily_zone_revenue.py` chạy được | ✅ Đã làm |
| *"ran jobs … on a Spark-on-YARN cluster"* | 28/10: Hadoop 3.5.0 tại `~/hadoop-3.5.0`, `10_revenue_cloud.py` chạy trên YARN, app `FINISHED / SUCCEEDED` (xem Phần 3) | ✅ **Đã làm** |
| *"connecting Spark to GCS/BigQuery"* | 28/10: job trên YARN đọc `gs://…/pq/` và ghi bảng `nytaxi.daily_zone_revenue_yarn` (91,280 dòng) qua spark-bigquery connector | ✅ **Đã làm** |
| Skills: *"HDFS fundamentals"* | 28/10: HDFS pseudo-distributed, file 717 MB thành 6 block, Spark trên YARN đọc `hdfs://` ra 6 task (xem Phần 4) | ✅ **Đã làm** |

Người phỏng vấn hỏi xoáy vào YARN gần như chắc chắn. Ngày 28/10 đã chạy thật cả YARN lẫn GCS → BigQuery, số liệu ở Phần 3. Hướng dẫn chi tiết đã có sẵn trong [day7-spark-cloud-yarn.html](day7-spark-cloud-yarn.html) (các bước 03 → 08), mất khoảng 1.5–2 giờ. **Nên làm Phần 3 trước khi đi phỏng vấn**, hoặc sửa CV cho đúng.

---

## Phần 1 — Giải phẫu job daily zone revenue

### Chạy

```bash
python 06_daily_zone_revenue.py \
  --input_green 'data/pq/green/2021/*' --input_yellow 'data/pq/yellow/2021/*' \
  --zones data/raw/zones/taxi_zone_lookup.csv \
  --output data/report/daily_zone_revenue --timezone America/New_York

# Đo stage qua REST API của Spark UI (script mới)
python 11_stage_probe.py
```

Muốn xem Spark UI bằng mắt thì thêm `input("Mở http://localhost:4040 rồi Enter...")` trước dòng `spark.stop()`, rồi xem tab **SQL / DataFrame** (nơi có ô `Exchange`) và tab **Stages**.

### Physical plan (rút gọn từ `explain()`)

```
BroadcastHashJoin [zone_id], [locationid]               ← join: BROADCAST
:- HashAggregate (final: count, sum, avg)               ← sau shuffle
:  +- Exchange hashpartitioning(PULocationID, day, service_type, 200)   ← SHUFFLE, cắt stage ở đây
:     +- HashAggregate (partial_count, partial_sum, partial_avg)        ← gộp trước ở từng partition
:        +- Union
:           :- Filter → FileScan parquet green  PushedFilters: [pickup >= …, pickup < …]
:           +- Filter → FileScan parquet yellow PushedFilters: [pickup >= …, pickup < …]
+- BroadcastExchange                                    ← bảng zones (265 dòng) gửi tới mọi executor
   +- FileScan csv taxi_zone_lookup
```

### Các stage đo được (`11_stage_probe.py`, broadcast + AQE bật)

| Stage | Task | Shuffle write | Shuffle read | Làm gì |
|---|---|---|---|---|
| 0, 1 | 1 | – | – | Đọc metadata/schema Parquet (green, yellow) |
| 2, 3 | 1 | – | – | Đọc header CSV và `inferSchema` của bảng zones (266 dòng tính cả header) |
| 4 | 1 | – | – | Dựng bảng broadcast từ 265 zone |
| **5** | **21** | **6.5 MB** | – | **Map side:** đọc 15,571,166 dòng, lọc, `partial_count/sum/avg`, ghi shuffle |
| **7** | **6** | – | **6.5 MB** | **Reduce side:** aggregate cuối, broadcast join, ghi Parquet |

**Những điều cần nói được:**
1. **Vì sao stage 5 có 21 task?** Vì green có 10 partition đầu vào và yellow có 11. 1 partition ứng với 1 task.
2. **Stage bị cắt ở đâu?** Tại `Exchange`, tức chỗ `groupBy` cần gom các dòng cùng key về một chỗ (wide transformation).
3. **Vì sao shuffle 15.5 triệu dòng mà chỉ có 6.5 MB?** Nhờ `partial_*` aggregate: mỗi partition tự gộp trước, chỉ gửi đi một dòng cho mỗi (zone, ngày, service_type) thay vì từng chuyến xe. Đây chính là map-side combine, giống `reduceByKey`.
4. **Vì sao plan ghi 200 mà stage 7 chỉ có 6 task?** AQE thấy shuffle chỉ có 6.5 MB nên gộp 200 partition xuống còn 6. `explain()` in plan **trước** khi AQE can thiệp (`isFinalPlan=false`), nên muốn thấy số thật phải xem Spark UI hoặc REST API sau khi chạy.
5. **Join là broadcast hay sort-merge?** Broadcast: thấy `BroadcastExchange` + `BroadcastHashJoin`, vì bảng zones nhỏ (265 dòng, ~12 KB). Join này không làm phát sinh shuffle bên phía bảng lớn.
6. **`PushedFilters`:** điều kiện lọc theo ngày được đẩy xuống tầng đọc Parquet, nên những row group nằm ngoài khoảng ngày bị bỏ qua.

### So sánh 3 cấu hình (cùng một job)

| Cấu hình | Số stage | Các stage reduce | Shuffle |
|---|---|---|---|
| Broadcast join + AQE (mặc định) | 7 | 6 task | 6.5 MB |
| Sort-merge join + AQE (`autoBroadcastJoinThreshold=-1`) | 8 | 6 task + **thêm 1 stage** 2 task cho join | 6.5 MB + **2.6 MB** |
| Sort-merge join, **không** AQE | 8 | **200 task + 200 task** | 6.5 MB + 5.1 MB |

- Bỏ broadcast thì phải **shuffle thêm một lần nữa** để join, sinh thêm 1 stage.
- Tắt AQE thì quay về đúng 200 partition như trong video khóa học (Spark 3.0), tức 200 task rất nhỏ và 200 file output.
- Thời gian chạy (1–4 giây) **không** dùng để so sánh được ở kích thước dữ liệu này. Cái cần nói là **số stage và lượng shuffle**.

---

## Phần 2 — RDD vs DataFrame

Cùng một phép tính: tổng doanh thu theo zone trên 15 triệu dòng yellow (263 zone).

| Cách viết | Thời gian |
|---|---|
| DataFrame `groupBy().agg(sum)` | **0.3 s** |
| RDD `map → reduceByKey` | 8.5 s |
| RDD `map → groupByKey → mapValues(sum)` | 8.1 s |

**Giải thích, đây là câu hay bị hỏi:**
- DataFrame chạy hoàn toàn trong JVM, được Catalyst tối ưu (chỉ đọc 2 cột) và Tungsten sinh code. **RDD trong PySpark phải chuyển từng dòng sang tiến trình Python** (serialize qua lại giữa JVM và Python), nên chậm hơn khoảng 25 lần.
- `reduceByKey` và `groupByKey` ở đây chạy gần bằng nhau, vì chỉ có 263 key và phần lớn thời gian nằm ở việc chuyển dữ liệu sang Python. Với nhiều key hoặc giá trị lớn, `groupByKey` sẽ chậm hơn hẳn và dễ gây OOM, vì nó shuffle **toàn bộ** giá trị thay vì gộp trước ở từng partition.
- **Trả lời câu "vì sao CV ghi cả RDD lẫn DataFrame":** *"Tôi dùng DataFrame cho mọi phép biến đổi chính vì nó được tối ưu. RDD tôi dùng để hiểu cơ chế bên dưới (partition, `reduceByKey` so với `groupByKey`), và dùng `mapPartitions` cho logic theo từng batch, ví dụ khởi tạo tài nguyên một lần cho mỗi partition. Tôi đã đo: cùng một phép tổng hợp, RDD trong PySpark chậm hơn khoảng 25 lần, do chi phí serialize giữa JVM và Python."*

Chạy thêm các lab cũ để xem lại: `python 07_cluster_anatomy.py`, `python 08_rdds.py`, `python 09_map_partitions.py`.

---

## Phần 3 — YARN + GCS + BigQuery

Làm theo [day7-spark-cloud-yarn.html](day7-spark-cloud-yarn.html), các bước 03 → 08. Tóm tắt:

```bash
# 1. Đưa dữ liệu lên GCS và tải connector
gcloud storage cp -r data/pq/green data/pq/yellow gs://de-zoomcamp-500308-nytaxi/pq/
mkdir -p lib && gcloud storage cp gs://hadoop-lib/gcs/gcs-connector-3.1.9.jar lib/

# 2. Hadoop 3.5.0 → ~/hadoop-3.5.0, sửa yarn-site.xml (8 GB, 6 vcore), rồi bật YARN
yarn --daemon start resourcemanager && yarn --daemon start nodemanager
yarn node -list            # 1 node RUNNING; UI tại http://localhost:8088

# 3. Submit: đọc GCS, chạy trên YARN, ghi BigQuery
spark-submit --master yarn --deploy-mode client \
  --num-executors 2 --executor-cores 2 --executor-memory 2g \
  --jars lib/gcs-connector-3.1.9.jar \
  --packages com.google.cloud.spark:spark-4.0-bigquery:0.46.0 \
  10_revenue_cloud.py ...
```

### Kết quả chạy thật ngày 28/10/2026

Lệnh (dữ liệu local, chạy trên YARN):

```bash
source env.sh                     # đã thêm HADOOP_HOME, YARN_CONF_DIR
yarn --daemon start resourcemanager && yarn --daemon start nodemanager
spark-submit --master yarn --deploy-mode client \
  --num-executors 2 --executor-cores 2 --executor-memory 2g \
  10_revenue_cloud.py \
    --input_green "file://$PWD/data/pq/green/2021/*" --input_yellow "file://$PWD/data/pq/yellow/2021/*" \
    --zones "file://$PWD/data/raw/zones/taxi_zone_lookup.csv" --timezone America/New_York \
    --output "file://$PWD/data/report/yarn_local"   # file:// vì sau Phần 4 đường dẫn mặc định là HDFS
```

| Quan sát | Giá trị đo được | Ứng với mũi tên |
|---|---|---|
| App | `application_1793120820022_0002`, queue `root.default`, `FINISHED / SUCCEEDED`, 15.8 s | ① |
| Container AM | `<memory:1024, vCores:1>` (client mode: 512 MB + 384 MB overhead = 896, làm tròn lên 1,024) | ② |
| Container executor | 2 × `<memory:3072, vCores:1>`: đúng phép tính 2,048 + 384 = 2,432, làm tròn lên 3,072 | ③ ④ |
| Block manager đăng ký với driver | 3: driver `192.168.1.24` và 2 executor `localhost` | ⑤ |
| Kết quả | 91,280 dòng, giống hệt khi chạy local | |
| Tài nguyên đã dùng | 89,997 MB-giây, 38 vcore-giây | |

**Vì sao YARN ghi `vCores:1` dù xin `--executor-cores 2`?** CapacityScheduler mặc định dùng `DefaultResourceCalculator`, chỉ xếp lịch theo RAM nên báo vCores là 1. Spark vẫn chạy 2 task song song trong mỗi executor vì số này do Spark quyết định. Muốn YARN tính cả CPU thì đổi sang `DominantResourceCalculator` trong `capacity-scheduler.xml`. Đây là câu trả lời ăn điểm nếu bị hỏi "sao trên :8088 thấy vCores khác với lúc submit".

**Bẫy master viết cứng, đã thử thật:** `spark-submit --master yarn 06_daily_zone_revenue.py ...` vẫn chạy xong và in ra kết quả, nhưng `yarn application -list -appStates ALL` vẫn đếm 2 app như trước khi chạy. Job đã chạy local mà không báo gì.

### Đọc GCS, chạy trên YARN, ghi BigQuery (28/10/2026)

```bash
source env.sh
B=gs://de-zoomcamp-500308-nytaxi
spark-submit --master yarn --deploy-mode client \
  --num-executors 2 --executor-cores 2 --executor-memory 2g \
  --jars lib/gcs-connector-3.1.9.jar \
  --packages com.google.cloud.spark:spark-4.0-bigquery:0.46.0 \
  10_revenue_cloud.py \
    --input_green "$B/pq/green/2021/*" --input_yellow "$B/pq/yellow/2021/*" \
    --zones "$B/zones/taxi_zone_lookup.csv" --timezone America/New_York \
    --format bigquery --output nytaxi.daily_zone_revenue_yarn
```

| Quan sát | Giá trị |
|---|---|
| App | `application_1793120820022_0006`, `FINISHED / SUCCEEDED` |
| Thời gian | 4 phút 54 giây, so với 15.8 giây khi đọc file local |
| Bảng BigQuery | `nytaxi.daily_zone_revenue_yarn`: 91,280 dòng, 263 zone, 7.8 MB, 9 cột |
| Tổng doanh thu | 294,750,068 USD |
| Ghi | `writeMethod=indirect`: Parquet tạm trên bucket, sau đó 1 load job (miễn phí), file tạm được tự xoá |

**Vì sao chậm gần 20 lần?** Có hai lý do. (1) Đọc từ GCS qua mạng nhà, không có data locality. (2) Job có 3 action (`count`, `show`, `write`) mà không `cache()`, nên 428 MB bị đọc lại từ GCS 3 lần. Câu trả lời khi bị hỏi cách tối ưu: `result.cache()` trước các action, hoặc bỏ `count`/`show` ở bản production. Trên Dataproc, cụm nằm cùng region với bucket nên đọc nhanh hơn nhiều.

**Vì sao trong BigQuery cột `day` là `2021-01-01 05:00:00`?** Job chia ngày theo giờ New York. 0 giờ New York (UTC−5) là 5 giờ UTC, và kiểu `TIMESTAMP` của BigQuery luôn lưu theo UTC. Dữ liệu không sai, chỉ hiển thị theo UTC. Nếu muốn hiển thị đúng ngày thì nên ghi cột kiểu `DATE`: `to_date(date_trunc('day', pickup_datetime))`.

**Sự cố thứ 2 khi đọc `gs://`:** `NoSuchMethodError: VectoredReadUtils.validateRangeRequest`. Parquet 1.17 dùng kiểu đọc vectored (nhiều đoạn của file trong một lần). Khi đó connector 3.1.9 gọi một hàm của Hadoop 3.4 đã bị bỏ trong Hadoop 3.5. Lỗi chỉ xuất hiện lúc executor đọc dữ liệu thật; liệt kê file và đọc schema vẫn chạy. Cách sửa: `spark.hadoop.parquet.hadoop.vectored.io.enabled=false`.

**Sự cố xác thực:** `Your default credentials were not found`. Lệnh `gcloud auth login` chỉ đăng nhập cho CLI. Thư viện (connector) cần **ADC**: `gcloud auth application-default login`.

**Sự cố thứ 1 khi đọc `gs://`:** `NumberFormatException: For input string: "64m"`. Spark 4.2 đóng gói Hadoop 3.5.0, và `core-default.xml` của bản này có sẵn mặc định `fs.gs.block.size=64m` (cùng 4 khóa `fs.gs.*` khác có đuôi `m`). GCS connector 3.1.9 đọc các khóa đó bằng `getLong`, nên không hiểu đuôi `m`. Cách sửa trong `10_revenue_cloud.py` là ghi đè bằng số byte thuần. Đây là ví dụ thật về xung đột phiên bản giữa thư viện và runtime.

**Sự cố gặp phải:** `getExecutorMemoryStatus()` trả về Map của Scala, gọi `.keySet().toArray()` qua Py4J sẽ lỗi. Dùng `.keys().mkString(",")` rồi tách chuỗi.

**Bẫy cần nhớ:** script `06_daily_zone_revenue.py` có `.master("local[*]")` cứng trong code. Cấu hình trong code **thắng** cấu hình truyền qua `spark-submit --master yarn`, nên job vẫn chạy local, không báo lỗi gì, và không hiện trên trang :8088. Phải bỏ dòng đó đi (đây là lý do có `10_revenue_cloud.py`).

### Giải thích từng tham số `spark-submit` (câu hỏi chắc chắn gặp)

| Tham số | Ý nghĩa | Giá trị trong lab |
|---|---|---|
| `--master yarn` | Dùng YARN làm cluster manager. Địa chỉ ResourceManager được đọc từ `YARN_CONF_DIR` | yarn |
| `--deploy-mode` | `client`: driver chạy trên máy gõ lệnh. `cluster`: driver chạy trong ApplicationMaster bên trong cụm, hợp với production | client |
| `--num-executors` | Số executor (số container YARN cấp cho executor) | 2 |
| `--executor-cores` | Số task mỗi executor chạy song song cùng lúc | 2, tức tối đa 4 task song song |
| `--executor-memory` | Heap JVM của mỗi executor | 2g |
| `--jars` / `--packages` | Thêm thư viện: jar có sẵn trên máy, hoặc tải từ Maven | GCS connector / BigQuery connector |

**Vì sao xin 2g mà YARN báo container 3 GB?** Spark cộng thêm overhead `max(384 MB, 10% × 2048)` = 384 MB, thành 2,432 MB. YARN cấp RAM theo bội số của `minimum-allocation-mb` (1,024), nên làm tròn lên **3,072 MB**. Với 8 GB trên node: 2 executor × 3 GB + AM 1 GB = 7 GB vừa đủ. Xin `--num-executors 3` thì cần 10 GB > 8 GB, nên YARN chỉ cấp được 2.

**Đọc GCS khác đọc file local ở chỗ nào?** Spark chỉ biết `file://` và `hdfs://`. Muốn đọc `gs://` phải có **GCS connector** (lớp `GoogleHadoopFileSystem`) và credential, nếu không sẽ gặp lỗi `No FileSystem for scheme "gs"`. Lưu trữ nằm tách rời khỏi tính toán, nên cụm tắt đi thì dữ liệu vẫn còn.

**Ghi BigQuery thế nào?** Dùng spark-bigquery connector: `df.write.format("bigquery").option("temporaryGcsBucket", ...)`. Chế độ indirect ghi tạm ra Parquet trên GCS rồi chạy load job vào BigQuery. Chế độ `direct` dùng Storage Write API, không cần bucket tạm.

---

## Phần 4 — Hadoop/HDFS

Có 2 mức. Mức 1 đã làm ngày 28/10/2026, kết quả ngay bên dưới.

**Mức 1 — Chạy thật (khoảng 30 phút, làm sau Phần 3 vì dùng chung bản Hadoop đã cài):** bật HDFS pseudo-distributed trên cùng `~/hadoop-3.5.0` (`core-site.xml`: `fs.defaultFS=hdfs://localhost:9000`; `hdfs-site.xml`: `dfs.replication=1`), rồi:

```bash
hdfs namenode -format
hdfs --daemon start namenode && hdfs --daemon start datanode
hdfs dfs -mkdir -p /taxi && hdfs dfs -put data/raw/fhvhv_tripdata_2021-01.csv /taxi/
hdfs dfs -ls -h /taxi
hdfs fsck /taxi/fhvhv_tripdata_2021-01.csv -files -blocks -locations
```

File fhvhv 717 MB sẽ được chia thành **6 block** (5 block 128 MB và 1 block 77 MB), mỗi block có 1 bản sao vì cụm chỉ có 1 DataNode. Khi đọc file này bằng `spark.read.csv("hdfs://localhost:9000/taxi/...")`, Spark sẽ tạo khoảng 6 task, mỗi task ứng với 1 block.

### Kết quả Mức 1 (28/10/2026)

Cấu hình: `core-site.xml` đặt `fs.defaultFS=hdfs://localhost:9000`. `hdfs-site.xml` đặt `dfs.replication=1` và để thư mục NameNode/DataNode ở `~/hadoop-data/`, không để mặc định trong `/tmp` vì macOS dọn `/tmp` là mất metadata.

```
$ hdfs fsck /taxi/fhvhv_tripdata_2021-01.csv -files -blocks -locations
/taxi/fhvhv_tripdata_2021-01.csv 752335705 bytes, replicated: replication=1, 6 block(s):  OK
0. blk_1073741825 len=134217728 Live_repl=1 [127.0.0.1:9866]
...
5. blk_1073741830 len=81247065  Live_repl=1 [127.0.0.1:9866]
Status: HEALTHY
```

Đọc bằng Spark trên YARN ([12_hdfs_probe.py](../../06-batch/practice/12_hdfs_probe.py)):

| Cách đọc | Số partition / task | Ghi chú |
|---|---|---|
| `sc.textFile` (RDD) | **6** | InputFormat của Hadoop: mỗi split là 1 block |
| `spark.read.csv`, `maxPartitionBytes` = 128 MB (mặc định) | **6** | Trùng với số block vì split 128 MB = block 128 MB |
| `spark.read.csv`, `maxPartitionBytes` = 64 MB | **12** | Vẫn 6 block nhưng thành 12 task |

Số dòng: 11,908,468 (RDD đếm 11,908,469 vì tính cả dòng header). Upload 717 MB lên HDFS mất 1.9 giây.

**Kết luận để nói khi phỏng vấn:** với DataFrame, **số task do kích thước split quyết định** (`spark.sql.files.maxPartitionBytes`), không do số block. Block chỉ cho driver biết **nên chạy task ở máy nào** (preferred location, tức data locality). Câu "1 block = 1 task" chỉ đúng với RDD/InputFormat của Hadoop, hoặc khi split size trùng với block size.

**Locality:** Spark UI ghi các task đọc ở mức `PROCESS_LOCAL`/`NODE_LOCAL`, không có task nào `ANY`. Trên cụm 1 node thì việc này hiển nhiên vì mọi block đều nằm trên chính máy chạy executor. Trên cụm thật nhiều node, task chờ được xếp vào máy có block trong `spark.locality.wait` (mặc định 3 giây); quá thời gian đó mới chạy ở máy khác (`ANY`) và kéo block qua mạng.

**Lưu ý sau khi bật HDFS:** vì `fs.defaultFS` giờ là HDFS, đường dẫn không có scheme như `/Users/.../data` sẽ bị hiểu là đường dẫn trên HDFS. Đọc file trên máy thì phải ghi rõ `file:///Users/...`. Spark trên YARN cũng đặt thư mục staging `.sparkStaging` trên HDFS.

**Sự cố:** trên YARN, gọi REST API qua `:4040` sẽ bị `AmIpFilter` chuyển hướng ra trang HTML, gây `JSONDecodeError`. Phải gọi qua proxy của RM: `http://localhost:8088/proxy/<appId>/api/v1/...`.

Bật và tắt:

```bash
hdfs --daemon start namenode && hdfs --daemon start datanode   # UI NameNode: http://localhost:9870
hdfs --daemon stop datanode && hdfs --daemon stop namenode
```

**Mức 2 — Tối thiểu:** vẽ lại sơ đồ dưới đây ra giấy và giải thích được từng mũi tên. Mũi tên được đánh số theo thứ tự chúng xảy ra khi chạy một job.

```
                      ① spark-submit
 ┌────────────────┐ ──────────────────▶ ┌──────────────────────┐
 │ Client (Mac)   │                     │ ResourceManager      │ :8088
 │ DRIVER         │                     │ (chia RAM/CPU cả cụm)│
 │ (client mode)  │                     └───┬──────────▲───────┘
 └──┬─────────▲───┘          ② bật AM       │          │ ③ AM xin container
    │         │                             ▼          │
    │         │   ┌── Máy worker: NodeManager ─────────┴──────────────────┐
    │         │   │  ┌─────────────────────────────┐                      │
    │         │   │  │ ApplicationMaster           │                      │
    │         │   │  │ (chứa DRIVER nếu            │                      │
    │         │   │  │  cluster mode)              │                      │
    │         │   │  └──────────────┬──────────────┘                      │
    │         │   │                 │ ④ nhờ NodeManager bật executor      │
    │         │   │                 ▼                                     │
    │  ⑥ task │   │  ┌──────────────┐   ⑨ shuffle   ┌──────────────┐      │
    └─────────┼──▶│  │ Executor 1   │◀─────────────▶│ Executor 2   │      │
              └───┼──┤ 2 core · 3GB │               │ 2 core · 3GB │      │
     ⑤ đăng ký,   │  └──────┬───────┘               └──────┬───────┘      │
       trả kết quả└─────────┼──────────────────────────────┼──────────────┘
                            │ ⑦ block nằm ở đâu?           │ ⑧ đọc/ghi block
                            ▼                              ▼
          ┌─ HDFS ────────────────────────────────────────────────────────┐
          │  ┌────────────────────────┐       ┌────────────────────────┐  │
          │  │ NameNode               │       │ DataNode (mỗi máy)     │  │
          │  │ metadata: file → block │       │ block 128 MB           │  │
          │  │ → DataNode nào         │       │ × 3 bản sao (mặc định) │  │
          │  └────────────────────────┘       └────────────────────────┘  │
          │  Thay HDFS bằng GCS (gs://): không có NameNode, cần connector │
          └───────────────────────────────────────────────────────────────┘
```

**Cách nhớ bằng ví dụ đời thường:** cả cụm là một công ty xây dựng.

| Thành phần | Vai trò | Ví dụ đời thường |
|---|---|---|
| ResourceManager | Quản lý RAM/CPU của cả cụm, không chạy code | Phòng điều phối: chia người và máy móc cho các dự án |
| NodeManager | Mỗi máy một cái; bật/tắt container, báo cáo tài nguyên còn trống | Đội trưởng ở từng công trường |
| ApplicationMaster | Mỗi job một cái; thay mặt job đi xin tài nguyên | Quản lý dự án |
| Driver | Giữ kế hoạch (DAG), chia stage và task, gom kết quả | Kỹ sư trưởng cầm bản vẽ |
| Executor | Process chạy task trong một container | Tổ thợ |
| NameNode | Chỉ giữ "mục lục": file gồm block nào, block nằm ở máy nào | Sổ danh mục của kho |
| DataNode | Giữ dữ liệu thật theo block | Các kệ hàng trong kho |

**Giải thích từng mũi tên:**

| # | Từ → Đến | Mang theo cái gì | Nếu hỏng thì thấy gì |
|---|---|---|---|
| ① | Client → ResourceManager | Gói ứng dụng: file `.py`, jar, cấu hình, số executor, RAM, core | `Retrying connect to server 0.0.0.0:8032`: RM chưa chạy hoặc thiếu `HADOOP_CONF_DIR` |
| ② | ResourceManager → NodeManager | Lệnh bật **container đầu tiên** để chạy ApplicationMaster | App kẹt ở `ACCEPTED`: cụm không còn chỗ cho AM |
| ③ | ApplicationMaster → ResourceManager | Xin N container cho executor; RM trả về container trên máy nào | Xin 3 executor mà chỉ được 2: thiếu RAM (2×3 GB + AM 1 GB = 7 GB trên node 8 GB) |
| ④ | ApplicationMaster → NodeManager | Lệnh bật executor trong container đã được cấp | `Container killed by YARN for exceeding memory limits`: tăng `spark.executor.memoryOverhead` |
| ⑤ | Executor → Driver | Executor đăng ký với driver, sau đó gửi trạng thái và kết quả task | Client mode: tắt máy Mac hoặc mất mạng thì driver chết và **cả job chết** |
| ⑥ | Driver → Executor | Task: đoạn code và partition cần xử lý | Task lỗi được thử lại 4 lần (`spark.task.maxFailures`), quá số lần thì job fail |
| ⑦ | Executor → NameNode | Chỉ hỏi metadata: block nào, ở DataNode nào. **Dữ liệu không đi qua NameNode** | NameNode chết thì cả HDFS không đọc được (điểm chết duy nhất nếu không có HA) |
| ⑧ | Executor ↔ DataNode | Đọc/ghi dữ liệu thật, theo từng block | DataNode chết thì đọc bản sao khác; nếu replication=1 thì báo `missing blocks` |
| ⑨ | Executor ↔ Executor | Dữ liệu shuffle: sau `groupBy`/`join`, task đọc kết quả trung gian từ máy khác | `FetchFailedException`: executor giữ file shuffle đã chết, Spark chạy lại stage trước |

**Ba ý interviewer hay hỏi xoáy từ sơ đồ này:**

1. **Client mode khác cluster mode ở đâu?** Chỉ khác **driver nằm ở đâu**. Client mode: driver ở máy submit, thấy log ngay, hợp để debug, nhưng tắt máy thì job chết. Cluster mode: driver nằm trong ApplicationMaster trên cụm, hợp cho job production. Mũi tên ⑤ ⑥ vì thế đi tới Mac hoặc tới AM.
2. **Vì sao executor phải hỏi NameNode trước (⑦)?** Để biết block nằm ở máy nào, từ đó driver xếp task chạy ngay trên máy có dữ liệu (**data locality**): chuyển code đến dữ liệu thay vì chuyển dữ liệu đến code. GCS không có locality, mọi lần đọc đều qua mạng; bù lại lưu trữ tách khỏi tính toán, tắt cụm vẫn còn dữ liệu.
3. **ResourceManager có chạy code Spark không?** Không. RM chỉ chia tài nguyên. Driver lập kế hoạch, executor chạy task. Nếu RM chết giữa chừng, các executor đang chạy vẫn tiếp tục cho tới khi cần xin thêm container.

**Tự kiểm tra (không nhìn tài liệu):**

- [ ] Vẽ đủ 7 khối: Client, RM, NM, AM, Executor ×2, NameNode, DataNode.
- [ ] Đánh số đủ 9 mũi tên theo đúng thứ tự.
- [ ] Vẽ lại hai lần: một lần client mode, một lần cluster mode, chỉ ra driver dời đi đâu.
- [ ] Trả lời được: "Dữ liệu 717 MB có đi qua NameNode không?" (Không, chỉ metadata của 6 block.)

---

## Bài nói mẫu 2 phút (checkpoint)

> **(1) Job làm gì.** Job đọc green và yellow taxi 2021, khoảng **15.5 triệu dòng** Parquet, chuẩn hóa tên cột, gộp hai nguồn, tính số chuyến, doanh thu, quãng đường trung bình theo **zone × ngày × loại taxi**, rồi gắn tên zone và ghi ra Parquet.
>
> **(2) Chạy qua những stage nào.** Ngoài vài stage nhỏ đọc metadata, có 2 stage chính. **Stage 1** gồm 21 task, ứng với 21 partition đầu vào: đọc Parquet chỉ 4 cột cần dùng, đẩy bộ lọc ngày xuống tầng đọc, và **gộp trước một phần** ở từng partition. Nhờ vậy 15.5 triệu dòng chỉ còn **6.5 MB** dữ liệu shuffle. **Ranh giới stage nằm ở `Exchange`** của `groupBy`. **Stage 2** đọc dữ liệu shuffle, aggregate lần cuối, rồi join với bảng zones. Plan ghi 200 partition nhưng AQE gộp xuống còn **6 task** vì dữ liệu nhỏ.
>
> **(3) Join.** Bảng zones chỉ có 265 dòng nên Spark dùng **broadcast hash join**: gửi bảng nhỏ tới mọi executor, không phải shuffle bảng lớn. Tôi đã thử tắt broadcast: job thêm một stage và phải shuffle thêm 2.6 MB cho sort-merge join.
>
> **(4) Chạy trên YARN.** Tôi submit bằng `--master yarn` với 2 executor, mỗi executor 2 core và 2 GB. YARN cấp container 3 GB vì cộng thêm memory overhead rồi làm tròn theo bội 1 GB. Dữ liệu đọc từ GCS qua connector, kết quả ghi thẳng vào BigQuery. Lúc làm tôi gặp hai lỗi lệch phiên bản giữa GCS connector và Hadoop 3.5 (`"64m"` và `NoSuchMethodError` khi đọc vectored), và đã sửa bằng cấu hình.

---

## Checkpoint cuối ngày

- [ ] Chạy `11_stage_probe.py`, chỉ ra được stage nào có shuffle và vì sao lại là 21 task và 6 task.
- [ ] Giải thích được vì sao shuffle 15.5 triệu dòng mà chỉ có 6.5 MB.
- [ ] Nói được kết quả đo RDD và DataFrame, và lý do.
- [x] **Làm Phần 3**: thấy app `FINISHED / SUCCEEDED` trên :8088 và bảng có dữ liệu trong BigQuery (xong 28/10).
- [ ] Vẽ lại sơ đồ YARN + HDFS mà không nhìn tài liệu.
