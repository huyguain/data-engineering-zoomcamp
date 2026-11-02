"""Job A — doanh thu theo giờ, theo zone đón khách, bằng window TVF (TUMBLE 1 giờ).

Cửa sổ [10:00, 11:00) chỉ được tính và ghi ra khi watermark vượt 11:00.
Sau đó cửa sổ bị xoá khỏi state, nên event trễ tới sau thời điểm này bị BỎ (không cập nhật).
Ưu điểm: state nhỏ, mỗi cửa sổ ghi đúng một lần. Nhược điểm: mất event quá trễ.
"""

from common import create_rides_source, jdbc_options, make_env


def main():
    t_env = make_env()
    source = create_rides_source(t_env, group_id="zone-revenue-window")

    t_env.execute_sql(f"""
        CREATE TABLE zone_revenue_window (
            window_start   TIMESTAMP(3),
            window_end     TIMESTAMP(3),
            pu_location_id INT,
            num_trips      BIGINT,
            total_revenue  DOUBLE,
            avg_distance   DOUBLE,
            PRIMARY KEY (window_start, pu_location_id) NOT ENFORCED
        ) WITH ({jdbc_options("zone_revenue_window")})
    """)

    t_env.execute_sql(f"""
        INSERT INTO zone_revenue_window
        SELECT
            window_start,
            window_end,
            pu_location_id,
            COUNT(*)           AS num_trips,
            SUM(total_amount)  AS total_revenue,
            AVG(trip_distance) AS avg_distance
        FROM TABLE(
            TUMBLE(TABLE {source}, DESCRIPTOR(event_ts), INTERVAL '1' HOUR)
        )
        GROUP BY window_start, window_end, pu_location_id
    """)


if __name__ == "__main__":
    main()
