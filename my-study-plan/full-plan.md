# Kế hoạch học tập: Full Repo Data Engineering Zoomcamp (3 tuần, full-time)

**Bối cảnh:** Fullstack developer 6 năm kinh nghiệm, đã vững SQL cơ bản, lập trình, Docker/API. Mục tiêu: học hết toàn bộ kiến thức trong repo này, ưu tiên tốc độ nhờ nền tảng sẵn có.

**Nhịp học mỗi ngày:** Sáng = đọc lý thuyết (file `.md`). Chiều = chạy code thực hành ngay trong ngày đó, không dồn qua ngày khác. Cuối ngày = 1 checkpoint để tự kiểm tra đã hiểu chưa.

---

## TUẦN 1 — Docker/Terraform, Orchestration (Kestra)

### Ngày 1 — Docker & Pipeline cơ bản
- **Sáng:** `01-docker-terraform/01-introduction.md` → `09-docker-compose.md` (lướt nhanh vì đã biết Docker, chỉ đọc kỹ phần dockerize pipeline + Postgres trong Docker)
- **Chiều:** Thực hành dựng lại pipeline trong thư mục `01-docker-terraform/pipeline/` bằng Docker Compose, kết nối Postgres + pgAdmin
- **Checkpoint:** Chạy được `docker-compose up` và insert/query dữ liệu vào Postgres qua pgAdmin

### Ngày 2 — Terraform & GCP
- **Sáng:** `10-sql-refresher.md`, `11-cleanup.md`, `12-terraform-overview.md`, `13-gcp-overview.md`
- **Chiều:** Thực hành trong `01-docker-terraform/terraform/`: tạo GCS bucket + BigQuery dataset bằng Terraform
- **Checkpoint:** Chạy được `terraform init/plan/apply` tạo thành công resource trên GCP

### Ngày 3 — Workflow Orchestration là gì + Kestra cơ bản
- **Sáng:** `02-workflow-orchestration/01-what-is-workflow-orchestration.md` → `04-kestra-concepts.md`
- **Chiều:** Cài Kestra theo `03-installing-kestra.md`, nghịch UI, tạo flow đầu tiên
- **Checkpoint:** Chạy được 1 flow Kestra đơn giản trên local

### Ngày 4 — Kestra orchestrate pipeline thật
- **Sáng:** `05-orchestrate-python-code.md` → `08-scheduling-and-backfills.md`
- **Chiều:** Thực hành load taxi data vào Postgres qua Kestra (`flows/` có sẵn), thử scheduling + backfill
- **Checkpoint:** 1 flow tự động load dữ liệu taxi vào Postgres theo lịch

### Ngày 5 — ETL/ELT + Kestra với BigQuery + Deploy
- **Sáng:** `09-etl-vs-elt.md`, `10-setup-google-cloud-platform.md`, `11-load-taxi-data-to-bigquery.md`, `12-schedule-and-backfill-full-dataset.md`
- **Chiều:** Lướt nhanh `13-18` (AI copilot, RAG, deploy to cloud — không trọng tâm cho JD) + thực hành load full dataset taxi vào BigQuery
- **Checkpoint:** Phân biệt được ETL vs ELT bằng ví dụ cụ thể từ pipeline vừa làm

---

## TUẦN 2 — Data Warehouse, dbt, Bruin, Spark nhập môn

### Ngày 6 — BigQuery sâu
- **Sáng:** `03-data-warehouse/01-data-warehouse-and-bigquery.md` → `04-internals-of-bigquery.md`
- **Chiều:** Chạy `big_query.sql` và `big_query_hw.sql`, tự thử partitioning/clustering trên bảng taxi
- **Checkpoint:** Giải thích được khi nào dùng partition, khi nào dùng cluster

### Ngày 7 — dbt (Analytics Engineering) lý thuyết
- **Sáng:** Đọc toàn bộ file trong `04-analytics-engineering/` (core concepts, setup, refreshers)
- **Chiều:** Setup dbt project theo hướng dẫn, kết nối tới BigQuery đã có ở Tuần 1
- **Checkpoint:** Chạy được `dbt run` thành công lần đầu

### Ngày 8 — dbt thực hành đầy đủ
- **Sáng + Chiều:** Làm hết project `04-analytics-engineering/taxi_rides_ny/` (models, tests, docs)
- **Checkpoint:** Có bộ dbt models hoàn chỉnh + chạy `dbt test` pass

### Ngày 9 — Bruin (Data Platforms)
- **Sáng:** `05-data-platforms/01-introduction.md` → `05-bruin-cloud.md`
- **Chiều:** `06-core-concepts-projects.md` → `10-core-concepts-commands.md` + tự tạo 1 pipeline nhỏ bằng Bruin
- **Checkpoint:** Hiểu Bruin khác gì Kestra/dbt về vai trò trong stack

### Ngày 10 — Spark nhập môn
- **Sáng:** `06-batch/01-introduction-to-batch-processing.md` → `04-first-look-at-spark.md`
- **Chiều:** Cài Spark local theo `03-installing-spark.md`, thực hành DataFrame cơ bản + `05-spark-dataframes.md`, `06-preparing-taxi-data.md`, `07-sql-with-spark.md`
- **Checkpoint:** Viết được 1 script Spark đọc file taxi, query bằng Spark SQL

---

## TUẦN 3 — Spark nâng cao, Hadoop, Kafka/Streaming, Workshop, Project

### Ngày 11 — Spark Cluster Architecture + RDD
- **Sáng:** `08-anatomy-of-a-spark-cluster.md` (đọc kỹ — JD hỏi sâu phần kiến trúc), `09-groupby-in-spark.md`, `10-joins-in-spark.md`
- **Chiều:** `11-operations-on-spark-rdds.md`, `12-spark-rdd-mappartition.md` + tự thực hành groupBy/join trên data taxi
- **Checkpoint:** Vẽ lại được sơ đồ Driver/Executor/Cluster Manager bằng lời của mình

### Ngày 12 — Spark + Cloud + Hadoop/YARN
- **Sáng:** `13-connecting-to-google-cloud-storage.md` → `16-connecting-spark-to-bigquery.md`
- **Chiều:** Setup Hadoop/YARN theo `06-batch/setup/hadoop-yarn.md`, chạy thử `spark-submit --master yarn`
- **Checkpoint:** Chạy được 1 Spark job trên Dataproc hoặc YARN, đọc/ghi được GCS + BigQuery

### Ngày 13 — Kafka nhập môn
- **Sáng:** `07-streaming/01-introduction.md`, `02-redpanda.md`, `03-produce-messages-to-kafka.md`
- **Chiều:** `04-consume-messages-with-python.md`, `05-save-events-to-postgresql.md` + thực hành produce/consume thật
- **Checkpoint:** Chạy được producer + consumer Python, thấy message chảy qua Kafka/Redpanda

### Ngày 14 — Flink + Kafka nâng cao
- **Sáng:** `06-why-flink.md` → `09-offsets-earliest-vs-latest.md`
- **Chiều:** `10-aggregation-with-tumbling-windows.md` → `14-questions-and-answers.md` + thực hành windowing, late events
- **Checkpoint:** Giải thích được tumbling window và cách xử lý late event — đây là điểm hay bị hỏi khi phỏng vấn streaming

### Ngày 15 — Workshop dlt + Lên kế hoạch Final Project
- **Sáng:** `cohorts/2026/workshops/dlt.md` — học ingestion tool dlt
- **Chiều:** Đọc `projects/README.md` + `projects/datasets.md`, chọn dataset (ưu tiên domain tài chính/giao dịch), phác thảo kiến trúc pipeline end-to-end
- **Checkpoint:** Có bản thiết kế kiến trúc (ingestion → lake → warehouse → transform → dashboard) sẵn sàng để bắt tay code

---

## Sau Ngày 15: Final Project (1-2 tuần riêng)

Đây là phần **không nằm gọn trong 15 ngày** — cần làm thêm sau khi xong lý thuyết, vì đây là sản phẩm chứng minh năng lực khi phỏng vấn. Dùng chính kiến trúc đã phác thảo ở Ngày 15 để build thật: ingestion (dlt/Kestra) → data lake (GCS) → data warehouse (BigQuery) → transform (dbt) → dashboard.

## Giới hạn cần lưu ý

- Repo **chỉ có phần cài đặt YARN** (`06-batch/setup/hadoop-yarn.md`) và một đoạn khái niệm HDFS trong `08-anatomy-of-a-spark-cluster.md` — **không có Hive/HBase thực hành**. Cần học bổ sung ngoài repo nếu công việc mục tiêu đòi hỏi.
- Hết 15 ngày là mốc "hiểu và chạy được toàn bộ code mẫu", chưa phải "thành thạo thực chiến" — Final Project là bước củng cố bắt buộc.
- Kinh nghiệm banking/core banking thực tế không có gì thay thế được ngoài làm việc thật trong môi trường đó.
