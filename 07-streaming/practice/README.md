# Project 2 — Streaming doanh thu taxi: Kafka (Redpanda) → Flink → PostgreSQL

Phát lại 1 ngày dữ liệu yellow taxi thật (2/3/2021, 61.843 chuyến) vào Kafka, có cố ý chèn
event đến trễ. 3 job Flink SQL xử lý song song rồi ghi vào Postgres. Cuối cùng một script
đối chiếu kết quả streaming với kết quả tính batch trên cùng dữ liệu.

Xây trên nền workshop [07-streaming](../README.md) của Data Engineering Zoomcamp
(image Flink, ý tưởng TUMBLE + upsert). Phần tự làm: producer phát lại dữ liệu thật có
event trễ, Job B và Job C, script đối chiếu, thí nghiệm restart, và các lỗi ghi ở cuối file.

```
                         ┌─ Job A  TUMBLE 1h (window TVF) ──────► zone_revenue_window
producer.py ─► Redpanda ─┼─ Job B  GROUP BY giờ + upsert ───────► zone_revenue_upsert   ─► reconcile.py
 (CSV TLC)     rides x3  └─ Job C  lọc chuyến bất thường ───────► fare_alerts               (so với pandas)
               partition                                          (PostgreSQL :5433)
```

| Job | Kiểu | Event trễ sau khi cửa sổ đóng | State | Ghi Postgres |
|---|---|---|---|---|
| A `zone_revenue_window_job.py` | `TUMBLE` 1 giờ | **Bị bỏ** | Nhỏ, xoá khi cửa sổ đóng | 1 lần / cửa sổ |
| B `zone_revenue_upsert_job.py` | `GROUP BY FLOOR(ts TO HOUR)` | Vẫn cập nhật (upsert) | Giữ đến khi hết TTL 2 giờ | Nhiều lần / dòng (mini-batch 1s) |
| C `fare_alert_job.py` | Lọc từng event | Không áp dụng | Không có | Theo từng event, PK chống trùng |

Thông số chung ([common.py](src/jobs/common.py)): watermark `event_ts - 5s`,
checkpoint mỗi 10s, parallelism 3 bằng số partition, key Kafka là `pu_location_id`,
session timezone UTC, idle-timeout nguồn 30s.

## Chạy

```bash
cd 07-streaming/practice
make venv            # .venv Python 3.13: kafka-python, pandas, pyarrow, psycopg2
make up              # build image Flink (lần đầu ~5 phút), bật Redpanda, Postgres, Flink
make topic           # topic rides, 3 partition
make jobs            # nộp 3 job, xem ở http://localhost:8081
make produce         # phát lại 1 ngày trong ~2,4 phút (--speedup 600)
make reconcile       # đối chiếu với batch
make reset           # xoá sạch topic + bảng để chạy lại
```

Cần file `06-batch/practice/data/raw/yellow/2021/03/yellow_tripdata_2021_03.csv.gz`
(tải theo [README của 06-batch](../../06-batch/practice/README.md)), hoặc truyền `--source`.

## Kết quả đo (29/10/2026)

Lần chạy: 61.843 event, có 1.230 event (2%) bị giữ lại 10 phút event time mới gửi.
Giữa lần chạy, TaskManager bị `docker compose restart` khi đang xử lý dữ liệu khoảng 11h.

| Chỉ số | Kết quả |
|---|---|
| Job B so với batch, 24 giờ | Khớp **100%** số chuyến, doanh thu lệch 0,000000 |
| Job A so với batch, 23 giờ đã đóng | 60.897 / 61.064, **mất 167 event trễ** (mô phỏng dự đoán ≤ 202) |
| Job A giờ 23:00 | Chưa ghi ra: watermark cuối là 23:59:50, chưa vượt 24:00 |
| Job C so với pandas | 385 / 385 cảnh báo, không trùng sau restart (291 tiền âm, 85 quãng đường 0 mà tiền > 50$, 7 trên 200$, 2 quãng đường trên 100 dặm) |
| Restart TaskManager | Cả 3 job tự khôi phục từ checkpoint (19/19 checkpoint thành công), kết quả không mất, không nhân đôi |

Đọc kết quả:
- **Job A chỉ mất event trễ vắt qua mốc giờ.** Ví dụ event 10:55 tới khi luồng đã ở 11:05:
  cửa sổ 10:00–11:00 đã đóng nên event bị bỏ. Event 10:20 tới lúc 10:30 thì vẫn được tính.
- **Số mất thực tế (167) thấp hơn mô phỏng (202):** Flink tính watermark riêng từng
  partition rồi lấy min, nên watermark chung thấp hơn mô phỏng và đóng cửa sổ muộn hơn.
- **Giờ 10:00 không mất event nào:** đó đúng là lúc TaskManager restart. Sau khi khôi phục,
  job đọc dồn phần dữ liệu tồn trong Kafka. Watermark chỉ cập nhật định kỳ 200ms, không theo
  kịp, nên event trễ lọt vào trước khi cửa sổ đóng. Đây là hiệu ứng backfill ở mục lỗi 3.

## Lỗi gặp khi làm và cách sửa

1. **TaskManager tự thoát:** `Could not connect to <container-id>:6123`. Do thiếu
   `FLINK_PROPERTIES=jobmanager.rpc.address: jobmanager`, nên entrypoint của image lấy hostname
   container làm địa chỉ JobManager. Đã thêm biến này vào cả 2 service.
2. **`flink run -d` treo mãi:** `.wait()` sau `execute_sql` giữ client chờ job kết thúc, mà
   job streaming không bao giờ kết thúc. Đã bỏ `.wait()`; `execute_sql(INSERT ...)` vốn đã nộp
   job bất đồng bộ.
3. **Event trễ không bị bỏ dù lý thuyết nói phải bỏ:** lần đầu producer bắn cả ngày trong
   1,8s. Watermark của Flink sinh định kỳ (`pipeline.auto-watermark-interval` = 200ms),
   trong 200ms đó đã trôi qua vài giờ event time, nên Job A khớp 100% "nhờ may". Kết luận:
   **chạy backfill và chạy live cho kết quả khác nhau** với cùng một job windowed. Đã thêm
   `--speedup` để producer phát theo nhịp event time.
4. **Dữ liệu parquet bên 06-batch bị lùi 7 giờ:** giờ 0 trong
   `06-batch/practice/data/pq/yellow` ứng với giờ 7 trong CSV gốc. Spark đọc giờ không kèm
   múi giờ theo múi giờ máy (+07:00). Producer đã chuyển sang đọc CSV gốc. Pipeline Project 1
   **cần sửa riêng**, ví dụ đặt `spark.sql.session.timeZone=UTC`.
5. **Postgres cổng 5432 bị Postgres của module 01 chiếm:** map ra cổng 5433.

## Ý chính để kể khi phỏng vấn

- **Tình huống:** Cần doanh thu theo giờ, theo zone, gần thời gian thực, và phát hiện chuyến
  bất thường ngay khi xảy ra; dữ liệu có event đến trễ.
- **Hành động:**
  - Kafka 3 partition, key theo zone. Flink SQL tính theo event time, watermark 5s,
    checkpoint 10s, sink JDBC upsert theo PRIMARY KEY.
  - Viết 2 cách tính cùng một chỉ số (TUMBLE so với GROUP BY + upsert) để đo đánh đổi.
  - Viết script đối chiếu streaming với batch.
- **Kết quả:**
  - Bản upsert khớp batch 100% kể cả khi restart giữa chừng.
  - Bản TUMBLE mất 0,27% số chuyến (167 event trễ) nhưng state nhỏ và mỗi cửa sổ chỉ ghi
    một lần.
  - Job cảnh báo bắt đủ 385/385 chuyến bất thường.
- **Đánh đổi chọn cách nào:**
  - Báo cáo cần chính xác thì dùng upsert, hoặc TUMBLE kèm job batch cuối ngày đối soát lại.
  - Dashboard real-time chấp nhận sai số nhỏ thì dùng TUMBLE.
  - Watermark dài hơn (ví dụ 15 phút) thì mất ít event hơn nhưng kết quả ra chậm hơn.
- **Câu hỏi xoáy hay gặp:**
  - *Sao cửa sổ cuối không ra?* Watermark chỉ tiến khi có event mới. Hết dữ liệu thì cửa sổ
    23:00 chờ mãi.
  - *TTL của Job B có rủi ro gì?* TTL tính theo processing time. Event trễ hơn 2 giờ đến sau
    khi state đã bị xoá sẽ tính lại từ 0 và **ghi đè sai** dòng cũ.
  - *Exactly-once ở đâu?* Offset Kafka và state nằm chung checkpoint. Sink không có
    transaction nhưng ghi idempotent nhờ upsert, nên đạt hiệu quả exactly-once.
