# Nạp khi chạy spark-submit --master yarn hoặc đọc hdfs://
#   source env-yarn.sh
# spark-submit đọc địa chỉ ResourceManager trong YARN_CONF_DIR. Spark cũng nạp
# core-site.xml, nên đường dẫn không có scheme sẽ là HDFS: file trên máy phải
# ghi file:///...
source "$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)/env.sh"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export YARN_CONF_DIR="$HADOOP_CONF_DIR"
