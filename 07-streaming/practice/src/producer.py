"""Phát lại 1 ngày dữ liệu yellow taxi thật vào topic `rides`, có cố ý chèn event trễ.

- Nhịp gửi bám theo event time, tăng tốc --speedup lần (mặc định 600: 1 ngày ~ 2,4 phút).
  Bắn hết một lần (--speedup 0) thì watermark định kỳ 200ms của Flink không kịp
  chạy theo, gần như không event trễ nào bị bỏ -> không giống lúc chạy live.
- Thứ tự gửi = thứ tự event time, trừ một tỉ lệ nhỏ event bị "giữ lại" LATE_SECONDS
  (mô phỏng xe mất sóng, gửi bù sau). Event trễ có event time cũ hơn event đã gửi trước nó.
- Key = pu_location_id: mọi chuyến cùng zone vào cùng partition, giữ thứ tự trong zone.
- acks=all + idempotent producer: broker xác nhận rồi mới coi là gửi xong, gửi lại khi lỗi
  mạng không sinh bản trùng trong Kafka.
- Ghi kèm data/sent_events.parquet (đúng những gì đã gửi, theo thứ tự) để reconcile.py
  đối chiếu kết quả streaming với kết quả tính batch trên cùng dữ liệu.

Chạy: .venv/bin/python src/producer.py --date 2021-03-02 --speedup 600
"""

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[1]
# Đọc CSV gốc của TLC, KHÔNG đọc bản parquet trong 06-batch/practice/data/pq: bản đó bị
# Spark dịch lùi 7 giờ (đọc giờ "trần" theo múi giờ máy +07:00).
DEFAULT_SOURCE = ROOT.parents[1] / "06-batch/practice/data/raw/yellow/2021/03/yellow_tripdata_2021_03.csv.gz"
COLUMNS = ["PULocationID", "DOLocationID", "trip_distance", "total_amount", "tpep_pickup_datetime"]


def load_day(source: Path, day: str) -> pd.DataFrame:
    df = pd.read_csv(source, usecols=COLUMNS, parse_dates=["tpep_pickup_datetime"])
    start = pd.Timestamp(day)
    df = df[(df.tpep_pickup_datetime >= start) & (df.tpep_pickup_datetime < start + pd.Timedelta(days=1))]
    return pd.DataFrame({
        "pu_location_id": df.PULocationID.astype("int32"),
        "do_location_id": df.DOLocationID.astype("int32"),
        "trip_distance": df.trip_distance.astype("float64"),
        "total_amount": df.total_amount.astype("float64"),
        # Giờ New York không kèm múi giờ -> coi như UTC; job Flink cũng đặt session UTC.
        "pickup_ts_ms": df.tpep_pickup_datetime.astype("datetime64[ms]").astype("int64"),
    }).reset_index(drop=True)


def plan_send_order(df: pd.DataFrame, late_ratio: float, late_seconds: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    df = df.copy()
    df["is_late"] = rng.random(len(df)) < late_ratio
    # Event trễ được gửi khi "đồng hồ" của luồng đã đi qua event_time + late_seconds.
    df["send_key_ms"] = df.pickup_ts_ms + np.where(df.is_late, late_seconds * 1000, 0)
    df = df.sort_values(["send_key_ms", "pickup_ts_ms"], kind="stable").reset_index(drop=True)
    df["send_seq"] = np.arange(len(df))
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--date", default="2021-03-02")
    parser.add_argument("--late-ratio", type=float, default=0.02)
    parser.add_argument("--late-seconds", type=int, default=600)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--speedup", type=float, default=600,
                        help="1 giây thật = N giây event time; 0 = bắn hết ngay (kiểu backfill)")
    parser.add_argument("--limit", type=int, default=None, help="chỉ gửi N event đầu (để thử nhanh)")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="rides")
    args = parser.parse_args()

    df = plan_send_order(load_day(args.source, args.date), args.late_ratio, args.late_seconds, args.seed)
    if args.limit:
        df = df.head(args.limit)

    out = ROOT / "data/sent_events.parquet"
    out.parent.mkdir(exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"{len(df):,} event ngày {args.date}, trong đó {int(df.is_late.sum()):,} event trễ "
          f"{args.late_seconds}s -> {out.relative_to(ROOT)}")

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        acks="all",
        enable_idempotence=True,
        linger_ms=20,
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )

    fields = ["pu_location_id", "do_location_id", "trip_distance", "total_amount", "pickup_ts_ms"]
    t0 = time.time()
    first_key = int(df.send_key_ms.iloc[0])
    for i, (row, send_key) in enumerate(zip(df[fields].itertuples(index=False), df.send_key_ms)):
        if args.speedup:
            # Giữ nhịp: event có send_key cách event đầu X giây thì gửi sau X / speedup giây thật.
            wait = (send_key - first_key) / 1000 / args.speedup - (time.time() - t0)
            if wait > 0:
                time.sleep(wait)
        event = row._asdict()
        event = {k: (v.item() if hasattr(v, "item") else v) for k, v in event.items()}
        producer.send(args.topic, key=event["pu_location_id"], value=event)
        if (i + 1) % 10_000 == 0:
            print(f"  đã gửi {i + 1:,}")
    producer.flush()
    print(f"Xong {len(df):,} event trong {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
