-- Postgres chạy file này một lần, lúc khởi tạo volume lần đầu.
-- Bảng sink phải có sẵn: JDBC connector của Flink không tự tạo bảng.
-- PRIMARY KEY ở đây khớp với PRIMARY KEY ... NOT ENFORCED trong DDL Flink,
-- để connector sinh câu INSERT ... ON CONFLICT DO UPDATE (upsert).

-- Job A: window TVF (TUMBLE). Mỗi cửa sổ ghi ra đúng một lần khi watermark vượt window_end.
CREATE TABLE zone_revenue_window (
    window_start   TIMESTAMP NOT NULL,
    window_end     TIMESTAMP NOT NULL,
    pu_location_id INT       NOT NULL,
    num_trips      BIGINT    NOT NULL,
    total_revenue  DOUBLE PRECISION NOT NULL,
    avg_distance   DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, pu_location_id)
);

-- Job B: group aggregation thường theo giờ. Kết quả cập nhật liên tục, kể cả khi event trễ tới.
CREATE TABLE zone_revenue_upsert (
    window_start   TIMESTAMP NOT NULL,
    pu_location_id INT       NOT NULL,
    num_trips      BIGINT    NOT NULL,
    total_revenue  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, pu_location_id)
);

-- Job C: cảnh báo chuyến bất thường, ghi theo từng event.
CREATE TABLE fare_alerts (
    pickup_ts      TIMESTAMP NOT NULL,
    pu_location_id INT       NOT NULL,
    do_location_id INT       NOT NULL,
    trip_distance  DOUBLE PRECISION NOT NULL,
    total_amount   DOUBLE PRECISION NOT NULL,
    reason         TEXT      NOT NULL,
    PRIMARY KEY (pickup_ts, pu_location_id, do_location_id, total_amount, reason)
);
