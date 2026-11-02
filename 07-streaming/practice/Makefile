.PHONY: venv up down reset topic jobs produce reconcile psql

PY := .venv/bin/python
FLINK_RUN := docker compose exec -T jobmanager ./bin/flink run -d --pyFiles /opt/src/jobs -py

venv:
	python3.13 -m venv .venv
	.venv/bin/pip install -q -r requirements.txt

up:
	docker compose up --build -d --wait redpanda postgres
	docker compose up -d jobmanager taskmanager

down:
	docker compose down

# Xoá sạch topic + bảng Postgres để chạy lại từ đầu.
reset:
	docker compose down -v

# 3 partition, key = pu_location_id. Job chạy parallelism 3 = mỗi subtask 1 partition.
topic:
	docker compose exec -T redpanda rpk topic create rides -p 3 || true

jobs:
	$(FLINK_RUN) /opt/src/jobs/zone_revenue_window_job.py
	$(FLINK_RUN) /opt/src/jobs/zone_revenue_upsert_job.py
	$(FLINK_RUN) /opt/src/jobs/fare_alert_job.py

produce:
	$(PY) src/producer.py

reconcile:
	$(PY) src/reconcile.py

psql:
	docker compose exec postgres psql -U postgres
