#!/usr/bin/env bash
set -e
TAXI_TYPE=$1
YEAR=$2
PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
    FMONTH=$(printf "%02d" "${MONTH}")          # 1 → 01
    FILE_NAME="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
    URL="${PREFIX}/${TAXI_TYPE}/${FILE_NAME}"

    DIR="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    mkdir -p "${DIR}"                            # tạo cấu trúc thư mục

    echo "Downloading ${URL}..."
    curl -sSLf "${URL}" -o "${DIR}/${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
done