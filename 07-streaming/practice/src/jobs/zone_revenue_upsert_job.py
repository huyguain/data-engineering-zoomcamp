"""Job B — cùng chỉ số với Job A nhưng dùng GROUP BY thường trên giờ đã làm tròn.

Không có cửa sổ nào "đóng": mỗi event (kể cả event trễ vài phút) cập nhật lại dòng của
giờ đó, Flink phát ra bản ghi mới và JDBC sink upsert theo PRIMARY KEY.
Ưu điểm: không mất event trễ. Cái giá: state giữ mọi (giờ, zone) đến khi hết TTL,
và Postgres bị ghi nhiều lần cho cùng một dòng.
"""

from common import create_rides_source, jdbc_options, make_env


def main():
    t_env = make_env()
    config = t_env.get_config()
    # Không có TTL thì state tăng mãi. 2 giờ: quá 2 giờ không có event mới cho một
    # (giờ, zone) thì xoá; event trễ hơn mức đó sẽ bắt đầu lại từ 0 và ghi đè sai.
    config.set("table.exec.state.ttl", "2 h")
    # Gom cập nhật 1 giây một lần thay vì phát ra từng event, đỡ ghi Postgres.
    config.set("table.exec.mini-batch.enabled", "true")
    config.set("table.exec.mini-batch.allow-latency", "1 s")
    config.set("table.exec.mini-batch.size", "5000")

    source = create_rides_source(t_env, group_id="zone-revenue-upsert")

    t_env.execute_sql(f"""
        CREATE TABLE zone_revenue_upsert (
            window_start   TIMESTAMP(3),
            pu_location_id INT,
            num_trips      BIGINT,
            total_revenue  DOUBLE,
            PRIMARY KEY (window_start, pu_location_id) NOT ENFORCED
        ) WITH ({jdbc_options("zone_revenue_upsert")})
    """)

    t_env.execute_sql(f"""
        INSERT INTO zone_revenue_upsert
        SELECT
            CAST(FLOOR(event_ts TO HOUR) AS TIMESTAMP(3)) AS window_start,
            pu_location_id,
            COUNT(*)          AS num_trips,
            SUM(total_amount) AS total_revenue
        FROM {source}
        GROUP BY CAST(FLOOR(event_ts TO HOUR) AS TIMESTAMP(3)), pu_location_id
    """)


if __name__ == "__main__":
    main()
