"""Đối chiếu kết quả streaming trong Postgres với "sự thật" tính batch bằng pandas.

Dùng data/sent_events.parquet (producer ghi ra) làm nguồn sự thật:
- truth          : gom nhóm toàn bộ event, không quan tâm thứ tự đến -> đáp án đúng.
- expected_window: mô phỏng watermark để đoán Job A (TUMBLE) sẽ bỏ những event trễ nào.
Sau đó so với 3 bảng Postgres và in chênh lệch theo từng giờ.

Chạy: .venv/bin/python src/reconcile.py
"""

from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
HOUR_MS = 3_600_000
WATERMARK_DELAY_MS = 5_000


def load_sent() -> pd.DataFrame:
    df = pd.read_parquet(ROOT / "data/sent_events.parquet").sort_values("send_seq")
    df["window_start_ms"] = df.pickup_ts_ms // HOUR_MS * HOUR_MS
    # Watermark lúc event tới = event time lớn nhất đã gửi TRƯỚC nó - 5s.
    # (Flink tính theo từng partition rồi lấy min, nên thực tế watermark thấp hơn
    #  một chút -> số event bị bỏ thực tế có thể ít hơn con số dự đoán này.)
    prev_max = df.pickup_ts_ms.cummax().shift(1, fill_value=np.iinfo("int64").min)
    watermark = prev_max - WATERMARK_DELAY_MS
    # Cửa sổ [start, end) được bắn khi watermark >= end - 1ms; sau đó event của nó bị bỏ.
    df["dropped_by_window"] = watermark >= df.window_start_ms + HOUR_MS - 1
    return df


def by_hour(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("window_start_ms").agg(trips=("total_amount", "size"), revenue=("total_amount", "sum"))
    g.index = pd.to_datetime(g.index, unit="ms")
    return g


def query(sql: str) -> pd.DataFrame:
    with psycopg2.connect(host="localhost", port=5433, user="postgres", password="postgres", dbname="postgres") as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


def pg_by_hour(table: str) -> pd.DataFrame:
    df = query(f"SELECT window_start, SUM(num_trips) AS trips, SUM(total_revenue) AS revenue "
               f"FROM {table} GROUP BY window_start")
    return df.set_index("window_start")


def main():
    sent = load_sent()
    final_watermark = sent.pickup_ts_ms.max() - WATERMARK_DELAY_MS
    open_from = pd.to_datetime(final_watermark // HOUR_MS * HOUR_MS, unit="ms")

    truth = by_hour(sent)
    expected_a = by_hour(sent[~sent.dropped_by_window])
    job_a = pg_by_hour("zone_revenue_window")
    job_b = pg_by_hour("zone_revenue_upsert")

    hours = truth.index
    report = pd.DataFrame({
        "truth": truth.trips,
        "late_sent": by_hour(sent[sent.is_late]).trips.reindex(hours, fill_value=0),
        "A_expected": expected_a.trips.reindex(hours, fill_value=0),
        "A_actual": job_a.trips.reindex(hours).astype("Int64"),
        "B_actual": job_b.trips.reindex(hours).astype("Int64"),
    })
    report["A_lost"] = report.truth - report.A_actual
    report["B_diff"] = report.B_actual - report.truth
    report.index = report.index.strftime("%H:%M")
    report.index.name = "hour"

    print(f"Event đã gửi: {len(sent):,} | event trễ: {int(sent.is_late.sum()):,} | "
          f"dự đoán Job A bỏ: {int(sent.dropped_by_window.sum()):,}")
    print(f"Watermark cuối: {pd.to_datetime(final_watermark, unit='ms')} -> "
          f"cửa sổ từ {open_from:%H:%M} trở đi chưa đóng, Job A chưa ghi ra.\n")
    print(report.to_string())

    closed = report.index < open_from.strftime("%H:%M")
    print("\nTổng các giờ đã đóng:")
    print(f"  truth={report.truth[closed].sum():,}  A_actual={int(report.A_actual[closed].sum()):,}  "
          f"A_lost={int(report.A_lost[closed].sum()):,}  B_actual={int(report.B_actual[closed].sum()):,}")
    rev_gap = (job_b.revenue.reindex(truth.index) - truth.revenue).abs().max()
    print(f"  Job B lệch doanh thu lớn nhất theo giờ: {rev_gap:.6f}")

    alerts = query("SELECT reason, COUNT(*) AS n FROM fare_alerts GROUP BY reason ORDER BY n DESC")
    print("\nfare_alerts theo lý do:")
    print(alerts.to_string(index=False))


if __name__ == "__main__":
    main()
