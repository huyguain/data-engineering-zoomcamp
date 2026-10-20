# Nạp trước mỗi buổi:  source env.sh
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Python mà executor dùng để chạy UDF. Không set -> Spark gọi python 3.9
# hệ thống (không có pyspark) và UDF chết giữa chừng.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
export PYSPARK_PYTHON="$HERE/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PATH="$HERE/.venv/bin:$PATH"
