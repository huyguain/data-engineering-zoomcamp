"""Phần dùng chung cho cả 3 job: môi trường Flink, bảng nguồn Kafka, DDL sink Postgres."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

KAFKA_BOOTSTRAP = "redpanda:29092"
TOPIC = "rides"
PG_URL = "jdbc:postgresql://postgres:5432/postgres"

# Event được phép trễ tối đa bao nhiêu so với event mới nhất đã thấy trên cùng partition.
WATERMARK_DELAY = "INTERVAL '5' SECOND"


def make_env(parallelism: int = 3) -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    # Checkpoint mỗi 10s: chụp state + offset Kafka. Job chết thì khôi phục từ đây,
    # đọc lại Kafka từ offset đã lưu; sink upsert theo PK nên ghi lại không bị nhân đôi.
    env.enable_checkpointing(10 * 1000)
    # Bằng số partition của topic: mỗi subtask nguồn đọc đúng 1 partition.
    env.set_parallelism(parallelism)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    # Dữ liệu taxi là giờ New York nhưng không kèm múi giờ; producer đổi sang epoch ms
    # coi như UTC. Đặt session UTC để window_start ra đúng giờ như trong file gốc.
    t_env.get_config().set("table.local-time-zone", "UTC")
    # Partition nào im lặng 30s thì bỏ qua khi tính watermark chung,
    # nếu không một partition rỗng sẽ giữ watermark đứng yên và không cửa sổ nào đóng.
    t_env.get_config().set("table.exec.source.idle-timeout", "30 s")
    return t_env


def create_rides_source(t_env: StreamTableEnvironment, group_id: str) -> str:
    t_env.execute_sql(f"""
        CREATE TABLE rides (
            pu_location_id INT,
            do_location_id INT,
            trip_distance  DOUBLE,
            total_amount   DOUBLE,
            pickup_ts_ms   BIGINT,
            event_ts AS TO_TIMESTAMP_LTZ(pickup_ts_ms, 3),
            WATERMARK FOR event_ts AS event_ts - {WATERMARK_DELAY}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = '{group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    return "rides"


def jdbc_options(table_name: str) -> str:
    return f"""
        'connector' = 'jdbc',
        'url' = '{PG_URL}',
        'table-name' = '{table_name}',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    """
