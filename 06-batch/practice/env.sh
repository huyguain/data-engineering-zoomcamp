# Nạp trước mỗi buổi:  source env.sh
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Python mà executor dùng để chạy UDF. Không set -> Spark gọi python 3.9
# hệ thống (không có pyspark) và UDF chết giữa chừng.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
export PYSPARK_PYTHON="$HERE/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PATH="$HERE/.venv/bin:$PATH"

# Lệnh yarn/hdfs dùng được ở mọi nơi, tự đọc cấu hình trong $HADOOP_HOME/etc/hadoop.
# KHÔNG export HADOOP_CONF_DIR ở đây: Spark sẽ nạp core-site.xml (fs.defaultFS=hdfs://)
# và đường dẫn tương đối như data/pq/... bị hiểu thành hdfs://localhost:9000/user/...
# Chạy trên YARN thì dùng: source env-yarn.sh
export HADOOP_HOME="$HOME/hadoop-3.5.0"
export PATH="$HADOOP_HOME/bin:$PATH"
