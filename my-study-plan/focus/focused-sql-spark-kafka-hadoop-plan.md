# Kế hoạch học tập tập trung: SQL, Spark, Kafka, Hadoop (2 tuần, full-time)

**Bối cảnh:** Fullstack developer 6 năm kinh nghiệm, đã vững SQL cơ bản, lập trình, Docker/API. Mục tiêu: chỉ tập trung 4 mảng SQL, Spark, Kafka, Hadoop (bỏ qua Docker/Terraform, Kestra, Bruin) để chuẩn bị nhanh nhất cho vị trí liên quan Big Data/ETL.

**Nhịp học mỗi ngày:** Sáng = đọc lý thuyết (file `.md`). Chiều = chạy code thực hành ngay trong ngày đó. Cuối ngày = 1 checkpoint tự kiểm tra.

**Lịch trình:** 10 ngày làm việc (2 tuần).

---

## TUẦN 1 — SQL nâng cao + Spark

### Ngày 1 — SQL/Data Warehouse: BigQuery cơ bản
- **Sáng:** `03-data-warehouse/01-data-warehouse-and-bigquery.md`, `02-partitioning-vs-clustering.md`
- **Chiều:** Chạy `big_query.sql`, `big_query_hw.sql`; tự thử tạo bảng partitioned + clustered trên dataset taxi
- **Checkpoint:** Giải thích được khi nào dùng partition, khi nào dùng cluster, viết được 1 câu query tận dụng cả hai

### Ngày 2 — SQL/Data Warehouse: Best practices + Internals
- **Sáng:** `03-data-warehouse/03-bigquery-best-practices.md`, `04-internals-of-bigquery.md`
- **Chiều:** Thực hành tối ưu 1 query chậm (dùng `EXPLAIN`/execution plan trên BigQuery), so sánh trước/sau khi áp dụng best practice
- **Checkpoint:** Nêu được 3 lỗi SQL thường gặp làm chậm BigQuery và cách tránh

### Ngày 3 — SQL modeling nâng cao với dbt
- **Sáng:** Đọc nhanh các file trong `04-analytics-engineering/` (core concepts cần cho SQL modeling, bỏ qua phần ML)
- **Chiều:** Làm project `04-analytics-engineering/taxi_rides_ny/` — viết models, tests bằng SQL/dbt
- **Checkpoint:** Chạy `dbt run` + `dbt test` pass, giải thích được staging → intermediate → mart layer

### Ngày 4 — Spark nhập môn
- **Sáng:** `06-batch/01-introduction-to-batch-processing.md` → `04-first-look-at-spark.md`
- **Chiều:** Cài Spark local (`03-installing-spark.md`), thực hành `05-spark-dataframes.md`, `06-preparing-taxi-data.md`
- **Checkpoint:** Viết được script Spark đọc file CSV/Parquet taxi và in ra schema + vài dòng

### Ngày 5 — Spark SQL + GroupBy + Join
- **Sáng:** `07-sql-with-spark.md`, `09-groupby-in-spark.md`, `10-joins-in-spark.md`
- **Chiều:** Tự viết 1 job Spark: join 2 bảng taxi, groupBy theo vùng/ngày, tính aggregate (count, sum, avg)
- **Checkpoint:** Giải thích được Spark SQL chạy qua Catalyst optimizer khác gì so với BigQuery SQL

---

## TUẦN 2 — Spark nâng cao + Hadoop + Kafka/Streaming

### Ngày 6 — Spark Cluster Architecture + RDD
- **Sáng:** `08-anatomy-of-a-spark-cluster.md` (đọc kỹ — JD hỏi sâu phần kiến trúc), `11-operations-on-spark-rdds.md`
- **Chiều:** `12-spark-rdd-mappartition.md` + tự thực hành convert 1 đoạn DataFrame code sang RDD để so sánh
- **Checkpoint:** Vẽ lại sơ đồ Driver/Executor/Cluster Manager + giải thích partition trong Spark bằng lời của mình

### Ngày 7 — Spark + Cloud + Hadoop/YARN
- **Sáng:** `13-connecting-to-google-cloud-storage.md` → `16-connecting-spark-to-bigquery.md`
- **Chiều:** Setup Hadoop/YARN theo `06-batch/setup/hadoop-yarn.md`, chạy thử `spark-submit --master yarn`
- **Checkpoint:** Chạy được 1 Spark job đọc/ghi GCS + BigQuery, và chạy được trên YARN thay vì local mode

### Ngày 8 — Kafka nhập môn
- **Sáng:** `07-streaming/01-introduction.md`, `02-redpanda.md`, `03-produce-messages-to-kafka.md`
- **Chiều:** `04-consume-messages-with-python.md`, `05-save-events-to-postgresql.md` + thực hành viết producer/consumer Python thật
- **Checkpoint:** Chạy được producer gửi message → consumer nhận và lưu vào Postgres

### Ngày 9 — Flink + Kafka: Windowing & Late Events
- **Sáng:** `06-why-flink.md` → `09-offsets-earliest-vs-latest.md`
- **Chiều:** `10-aggregation-with-tumbling-windows.md`, `11-late-events-and-upserts.md`, `12-understanding-window-types.md`
- **Checkpoint:** Giải thích được tumbling window, cách Flink xử lý late event bằng upsert — điểm hay bị hỏi trong phỏng vấn streaming

### Ngày 10 — Tổng hợp: Build pipeline nhỏ Kafka → Flink → Postgres
- **Sáng:** `13-cleanup.md`, `14-questions-and-answers.md` (ôn lại các câu hỏi thường gặp)
- **Chiều:** Tự build 1 pipeline end-to-end nhỏ: Kafka producer → Flink job (window aggregate) → ghi kết quả vào Postgres, dùng chính code mẫu trong `07-streaming/code/`
- **Checkpoint:** Pipeline chạy được từ đầu đến cuối không lỗi — đây là demo tốt để nói trong phỏng vấn

---

## Giới hạn cần lưu ý

- Repo **chỉ có phần cài đặt YARN** (`06-batch/setup/hadoop-yarn.md`) để chạy Spark-on-YARN, và một đoạn khái niệm HDFS trong `08-anatomy-of-a-spark-cluster.md` — **không có Hive/HBase thực hành**. Nếu JD cần Hive/HBase thật sự, phải học bổ sung ngoài repo (tự dựng cụm Hadoop + Hive + HBase, hoặc dùng Cloudera/HDP sandbox).
- Lộ trình này **bỏ qua** Module 1 (Docker/Terraform), Module 2 (Kestra), Module 5 (Bruin) và Final Project — xem file `sql-spark-kafka-hadoop-plan.md` trong cùng thư mục nếu muốn học full repo.
- Hết 10 ngày là mốc "hiểu và chạy được code mẫu", chưa phải "thành thạo thực chiến" — nên làm thêm 1 project cá nhân (vd: pipeline xử lý dữ liệu giao dịch giả lập) sau khi học xong, mất thêm khoảng 1 tuần.
- Kinh nghiệm banking/core banking thực tế không có gì thay thế được ngoài làm dự án thật trong môi trường đó.
