# Data Engineering — Recommendation

> Personal notes — saved 2026-06-23.
> Câu hỏi gốc: "Cách tốt nhất recommend cho tôi" (học DE từ Loukus DB).

---

## Recommendation 1 dòng

**DataEng Zoomcamp (free, 9 tuần) + Loukus làm capstone project.**

Lý do: đã là fullstack dev → có nền SQL + Postgres + TypeScript → bỏ qua basics, đầu tư thẳng vào modern data stack (dbt, Airflow, BigQuery, Kafka, Spark). Zoomcamp dạy đúng phần đang thiếu, miễn phí, có community + portfolio.

---

## Kế hoạch cụ thể 10 tuần

### Tuần 0 — Chuẩn bị (1 buổi tối)

```bash
git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git
```

Tools cần có:
- Docker Desktop
- Google Cloud account (free tier $300 credit)
- GitHub repo cho portfolio (vd: `huy-data-engineering-portfolio`)

Đọc README → setup GCP project → enable BigQuery API.

### Tuần 1–2 — Containerization + Postgres + Terraform

Zoomcamp Week 1 dạy:
- Docker + docker-compose (Postgres + pgAdmin local)
- Terraform basics (provision GCP resources)
- Ingest NYC Taxi data vào Postgres

Skip nhanh phần SQL (đã biết), tập trung vào Terraform + Docker compose pattern.

### Tuần 3 — Workflow Orchestration (Airflow / Kestra)

Zoomcamp Week 2 — Airflow DAG basics, idempotent tasks, retries, scheduling.

**Bài tập real:** viết DAG `loukus_daily_etl` chạy nightly:
- Extract `carts`, `orders`, `shipments` từ Loukus
- Load thô vào BigQuery raw layer

### Tuần 4–5 — Data Warehouse + BigQuery (CORE)

Zoomcamp Week 3 — partitioning, clustering, query optimization trên BigQuery.

**Capstone setup:** thiết kế star schema cho Loukus:
```
fct_cart_lifecycle (1 row / cart)
fct_shipment (1 row / shipment)
dim_customer, dim_part, dim_machine, dim_date
```

→ Đây là **valuable IP** cho EGS — sau này có project DE nào đã có template.

### Tuần 6–7 — Analytics Engineering (dbt) — QUAN TRỌNG NHẤT

Zoomcamp Week 4 — dbt:
- staging → intermediate → marts
- dbt tests
- documentation auto-gen
- snapshots (SCD type 2)

**Capstone:**
```
loukus-dw/
├── models/
│   ├── staging/
│   │   ├── stg_carts.sql
│   │   ├── stg_orders.sql
│   │   └── stg_shipments.sql
│   ├── intermediate/
│   │   └── int_cart_with_split_lineage.sql   ← recursive CTE
│   └── marts/
│       ├── fct_cart_lifecycle.sql
│       ├── fct_scrap_metrics.sql
│       └── dim_*.sql
├── tests/
│   └── singular/
└── dbt_project.yml
```

→ Push GitHub. 60% portfolio strength.

### Tuần 8 — Stream (Kafka) — optional

Zoomcamp Week 6 — Kafka basics. Skip nếu time-strapped. Nếu làm:
- Debezium CDC từ Loukus Postgres
- Topic `carts_changes` → consumer push BigQuery near-realtime

### Tuần 9 — Batch (Spark) — optional

Zoomcamp Week 5 — Spark. Skip nếu Loukus volume nhỏ (vài GB).

Chỉ làm nếu muốn apply company dùng big data (telco, fintech).

### Tuần 10 — Visualization + portfolio polish

- Setup Metabase trên Loukus DW
- Build 3 dashboards: production health, customer, inventory
- Viết blog post: "Building a Data Warehouse for a Foundry MES — OLTP → Star Schema with dbt"
- Push LinkedIn

---

## Đầu ra portfolio sau 10 tuần

```
GitHub: huy-data-engineering-portfolio
├── README.md           ← architecture diagram + screenshots
├── infra/              ← Terraform code
├── airflow/            ← DAGs
├── dbt/                ← models + tests
├── docs/
│   ├── data-model.md   ← star schema design
│   └── post-mortem.md  ← lessons learned
└── notebooks/          ← exploratory queries
```

→ Đủ apply junior-mid DE role hoặc pitch internal DE work tại EGS.

---

## Tại sao chọn cái này thay vì lựa chọn khác

| So với... | DataEng Zoomcamp hơn vì |
|---|---|
| **DataCamp tracks** | Miễn phí thay vì $200+/năm; project-based; sửa code thật |
| **Udemy course** | Có community Slack 30k member, instructor response thật; cập nhật theo modern stack |
| **Tự build từ blog** | Có structure rõ, deadline tuần; không bỏ dở |
| **Sách Kimball** | Sách quá lý thuyết với fullstack dev — cần hands-on |
| **Boot camp $5k+** | Cái nào dạy cũng cùng tools; Zoomcamp free + structured tương đương |
| **Học tools đơn lẻ** | Không có thread xuyên suốt → kiến thức rời rạc |

---

## Cảnh báo

### Đừng mất thời gian vào

- Học hết Spark / Hadoop chuyên sâu (lỗi thời cho modern stack)
- Đọc hết sách Kimball trước khi code (lý thuyết quá nhiều)
- Master Snowflake VÀ BigQuery VÀ Redshift (chọn 1 — BigQuery rẻ nhất cho học)
- Tự build framework ETL (đã có dbt, Airflow — đừng reinvent)
- Học data science / ML (DE ≠ DS)

### Tập trung vào

- **dbt mastery** — hottest skill 2026
- **SQL window functions + CTE** — analytics SQL khác OLTP SQL
- **Dimensional modeling thinking** — không phải syntax, mà mindset
- **DAG design** — idempotent, backfillable

---

## Action 24h tới

1. **Tối nay (1h):** `git clone DataTalksClub/data-engineering-zoomcamp`, setup GCP free trial, đọc README
2. **Tuần này (10h):** hoàn thành Week 1 Zoomcamp (Docker + Terraform + ingest)
3. **Cuối tuần:** create repo `huy-de-portfolio`, push Week 1 deliverable
4. **Báo cáo PM tại EGS** (optional): "Đang upskill DE, có thể support project data tương lai"

→ 24h này quan trọng hơn 10 tuần kia. Đa số bỏ cuộc ở bước 1.

---

## Tóm tắt 3 dòng

1. **Học gì:** dbt + Airflow + BigQuery + dimensional modeling (4 thứ — không cần thêm)
2. **Học từ đâu:** DataEng Zoomcamp (free, 9 tuần, có community)
3. **Capstone:** Loukus OLTP → BigQuery DW với dbt → Metabase dashboards → push GitHub

**Bắt đầu tối nay với `git clone`. Đừng research thêm. Đừng so sánh thêm khoá. Just start.**
