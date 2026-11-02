"""Job C — cảnh báo chuyến bất thường theo từng event (không cần cửa sổ, không có state).

Đây là nhánh "real-time phát hiện bất thường" trong câu thiết kế hệ thống:
event vào là xét ngay, không phải chờ watermark.
"""

from common import create_rides_source, jdbc_options, make_env

# Quy tắc rút ra từ dữ liệu 2/3/2021: total_amount âm (hoàn tiền/huỷ),
# tiền rất cao, hoặc tiền cao mà quãng đường bằng 0.
ALERT_RULES = """
    CASE
        WHEN total_amount < 0                            THEN 'negative_amount'
        WHEN total_amount > 200                          THEN 'very_high_amount'
        WHEN trip_distance = 0 AND total_amount > 50     THEN 'zero_distance_high_amount'
        WHEN trip_distance > 100                         THEN 'very_long_trip'
    END
"""


def main():
    t_env = make_env()
    source = create_rides_source(t_env, group_id="fare-alert")

    t_env.execute_sql(f"""
        CREATE TABLE fare_alerts (
            pickup_ts      TIMESTAMP(3),
            pu_location_id INT,
            do_location_id INT,
            trip_distance  DOUBLE,
            total_amount   DOUBLE,
            reason         STRING,
            PRIMARY KEY (pickup_ts, pu_location_id, do_location_id, total_amount, reason) NOT ENFORCED
        ) WITH ({jdbc_options("fare_alerts")})
    """)

    t_env.execute_sql(f"""
        INSERT INTO fare_alerts
        SELECT pickup_ts, pu_location_id, do_location_id, trip_distance, total_amount, reason
        FROM (
            SELECT
                CAST(event_ts AS TIMESTAMP(3)) AS pickup_ts,
                pu_location_id,
                do_location_id,
                trip_distance,
                total_amount,
                {ALERT_RULES} AS reason
            FROM {source}
        )
        WHERE reason IS NOT NULL
    """)


if __name__ == "__main__":
    main()
