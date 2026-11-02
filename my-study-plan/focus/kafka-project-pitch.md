# Bài kể project Kafka khi phỏng vấn

Dùng cho phần **Đào sâu project** (30'). Mọi con số dưới đây đều là số đo thật trong lab, nguồn ở [07-streaming/practice/README.md](../../07-streaming/practice/README.md) và code trong [07-streaming/practice/src](../../07-streaming/practice/src/). Không thêm số nào ngoài các số này.

**Nguyên tắc:** nói rõ ngay câu đầu tiên rằng đây là project tự làm trên dữ liệu public NYC Taxi, dựa trên workshop streaming của Data Engineering Zoomcamp, chạy bằng Docker Compose trên máy cá nhân, broker là **Redpanda** (tương thích Kafka API). Nói trước thì người phỏng vấn đánh giá phần kỹ thuật; để họ tự phát hiện thì mất điểm tin cậy.

---

## Sơ đồ 1 — Kiến trúc (vẽ lên bảng khi bắt đầu kể)

```
                                   Kafka (Redpanda), topic "rides", 3 partition
  CSV TLC 2/3/2021                 key = pu_location_id
  61,843 chuyến          ┌───────────────────────────────┐
        │                │ p0 ▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕ │
        ▼                │ p1 ▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕ │
  ┌────────────┐  acks=all│ p2 ▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕▕ │
  │ producer.py│─────────►└───────────────┬───────────────┘
  │ speedup 600│ idempotent               │ 3 consumer group, mỗi job đọc đủ topic
  │ 2% trễ 10' │            ┌─────────────┼──────────────────────┐
  └─────┬──────┘            ▼             ▼                      ▼
        │          ┌──────────────┐ ┌──────────────────┐ ┌───────────────┐
        │          │ Job A        │ │ Job B            │ │ Job C         │
        │          │ TUMBLE 1h    │ │ GROUP BY giờ     │ │ lọc từng event│
        │          │              │ │ + upsert, TTL 2h │ │ 4 luật        │
        │          └──────┬───────┘ └────────┬─────────┘ └───────┬───────┘
        │      Flink: event time, watermark −5s, checkpoint 10s, parallelism 3
        │                 ▼                  ▼                   ▼
        │          ┌─────────────────────────────────────────────────────┐
        │          │ PostgreSQL  zone_revenue_window │ zone_revenue_upsert│
        │          │             fare_alerts   (upsert theo PRIMARY KEY)  │
        │          └──────────────────────────┬──────────────────────────┘
        │                                     │
        ▼                                     ▼
  sent_events.parquet ──────────────► reconcile.py (pandas, batch)
  ("đáp án đúng")                     so streaming với batch theo từng giờ
```

**Cách vẽ khi nói (3 bước, mỗi bước một câu):**
1. Vẽ hàng ngang `producer → Kafka 3 partition → Flink → Postgres`: *"Đây là luồng chính."*
2. Tách Flink thành 3 ô A, B, C: *"Cùng một topic, 3 job đọc độc lập bằng 3 consumer group."*
3. Vẽ đường vòng dưới cùng `parquet → reconcile`: *"Đây là phần tôi thấy giá trị nhất: so streaming với batch để chứng minh kết quả đúng."*

---

## Bản 30 giây (khi bị hỏi "kể nhanh project Kafka")

> Tôi xây một pipeline streaming tính doanh thu taxi New York **theo giờ, theo zone** và **cảnh báo chuyến bất thường** gần như ngay lập tức. Tôi phát lại 1 ngày dữ liệu thật, **61,843 chuyến**, vào Kafka, có cố ý chèn **2% event đến trễ**. Flink SQL xử lý theo event time rồi ghi upsert vào PostgreSQL. Điểm tôi tập trung là **chứng minh kết quả đúng**: tôi viết script đối chiếu streaming với batch và đo được rằng cách upsert khớp batch **100%** kể cả khi kill TaskManager giữa chừng, còn cách tumbling window mất **167 event trễ**, tức 0.27%.

---

## Bản đầy đủ theo STAR (~3 phút)

### S — Tình huống

> Bài toán là cần doanh thu theo giờ và theo zone đón khách **gần thời gian thực** cho dashboard, đồng thời phát hiện chuyến bất thường ngay khi nó xảy ra, ví dụ tiền âm hay 0 dặm mà thu hơn 50 đô. Khó ở chỗ dữ liệu thực tế luôn có **event đến trễ**: xe mất sóng rồi gửi bù sau. Nếu xử lý sai thì doanh thu bị thiếu, hoặc bị đếm trùng khi job khởi động lại.

### T — Nhiệm vụ

> Tôi đặt ra ba yêu cầu:
> 1. Xử lý theo **event time** và có chiến lược rõ ràng cho event trễ.
> 2. **Chịu được lỗi**: job chết giữa chừng thì không mất và không nhân đôi dữ liệu.
> 3. **Đo được độ đúng** bằng số, chứ không chỉ "chạy không lỗi".

### A — Hành động

**1. Producer mô phỏng luồng thật.**

> Producer đọc CSV gốc của TLC và phát lại **theo nhịp event time**: 1 giây thật bằng 10 phút dữ liệu, cả ngày phát trong khoảng 2.4 phút. 2% chuyến bị giữ lại 10 phút mới gửi để giả làm event trễ. Key là `pu_location_id`, nên cùng zone luôn vào cùng partition và giữ thứ tự trong zone. Producer bật `acks=all` và idempotence để không mất và không trùng trong Kafka. Mọi event đã gửi được ghi ra một file parquet để làm đáp án khi đối chiếu.

**2. Kafka và Flink.**

> Topic `rides` có 3 partition, Flink chạy parallelism 3 để mỗi subtask nguồn đọc đúng một partition. Cấu hình chung: watermark `event_ts − 5 giây`, checkpoint mỗi 10 giây, idle-timeout 30 giây, timezone UTC. Sink là JDBC vào Postgres, mỗi bảng có PRIMARY KEY nên connector sinh câu `INSERT ... ON CONFLICT DO UPDATE`.

**3. Cùng một chỉ số, hai cách tính, để đo đánh đổi.**

> - **Job A**: `TUMBLE` 1 giờ. Cửa sổ chốt khi watermark vượt mốc giờ, ghi một lần rồi xóa state. Event đến sau đó bị bỏ.
> - **Job B**: `GROUP BY` theo giờ đã làm tròn, không có cửa sổ nào đóng. Mỗi event, kể cả event trễ, cập nhật lại dòng trong Postgres bằng upsert. Đổi lại phải giữ state, nên tôi đặt TTL 2 giờ và mini-batch 1 giây để giảm số lần ghi.
> - **Job C**: lọc từng event theo 4 luật bất thường, không giữ state, không chờ watermark.

**4. Kiểm chứng.**

> Script `reconcile.py` tính batch bằng pandas trên đúng file đã gửi để ra đáp án, rồi so với 3 bảng Postgres theo từng giờ. Script còn **mô phỏng watermark** để dự đoán trước Job A sẽ bỏ bao nhiêu event. Giữa lần chạy, tôi `restart` TaskManager khi luồng đang ở khoảng 11 giờ để thử khả năng khôi phục.

### R — Kết quả

> - **Job B** khớp batch **100%** cả 24 giờ, doanh thu lệch 0.
> - **Job A**: 60,897 trên 61,064 chuyến của 23 giờ đã đóng, **mất 167 event**, đúng là những event trễ vắt qua mốc giờ. Mô phỏng dự đoán tối đa 202.
> - **Job C** bắt đủ **385/385** cảnh báo, không trùng sau restart.
> - Restart: **19/19 checkpoint** thành công, cả 3 job tự khôi phục, không mất, không nhân đôi.

### Bài học (nói ngắn, chọn 1–2 ý)

> - **Chọn cách tính theo mục đích**: dashboard chấp nhận sai số nhỏ thì TUMBLE (nhẹ, ghi 1 lần); báo cáo cần chính xác thì upsert, hoặc TUMBLE kèm job batch đối soát cuối ngày.
> - **Backfill và chạy live cho kết quả khác nhau**: lần đầu tôi bắn cả ngày trong 1.8 giây, Job A khớp 100% "nhờ may", vì watermark chỉ được sinh mỗi 200ms nên tụt lại hàng giờ event time. Tôi phải thêm `--speedup` để phát theo nhịp thật. Nếu không đối chiếu với batch thì tôi đã không phát hiện ra.

---

## Sơ đồ 2 — Một event trễ đi qua Job A và Job B

Dùng khi bị hỏi *"late event xử lý thế nào?"* Vẽ trục thời gian rồi chỉ vào từng mốc.

```
 event time  10:00                       10:55   11:00              11:05
   ──────────┼─────────────────────────────┼───────┼──────────────────┼──────►
             │◄──── cửa sổ [10:00, 11:00) ────────►│                  │
                                           ▲       ▲                  ▲
                         chuyến zone 237 xảy ra    │        chuyến đó mới tới Flink
                         lúc 10:55, $20            │        (watermark ≈ 11:04:55)
                                                   │
                            watermark vượt 11:00 → Job A chốt cửa sổ 10:00

  Job A (TUMBLE)        cửa sổ 10:00 đã ghi và xóa state → event BỊ BỎ   (1 trong 167)
  Job B (GROUP BY+upsert) state (10:00, 237) vẫn còn → +1 chuyến, +$20
                          → UPSERT ghi đè dòng cũ trong Postgres          (khớp batch)
  Job C (lọc)           $20 là bình thường → không cảnh báo
```

**Câu nói kèm:**
> Watermark là event time lớn nhất đã thấy trừ 5 giây. Event 10:55 tới khi luồng đã ở 11:05, nên với Job A cửa sổ đã đóng và event bị bỏ. Job B không có cửa sổ đóng nên vẫn cộng vào và ghi đè dòng trong Postgres.

---

## Sơ đồ 3 — Checkpoint và restart (exactly-once)

Dùng khi bị hỏi *"job chết giữa chừng thì có mất hay trùng không?"*

```
 thời gian thật ──────────────────────────────────────────────────────────►
                chk #7                 chk #8          💥 TaskManager restart
                  │                      │                    │
                  ▼                      ▼                    ▼
  Flink   ──[offset p0..p2 + state]──[offset + state]──xử lý tiếp──X
                                         │                    │
                                         └──── khôi phục ◄────┘
                                               từ chk #8
                                               đọc lại Kafka từ offset trong chk #8

  Đoạn xử lý giữa chk #8 và lúc chết bị làm lại → Postgres nhận lại vài dòng
  → sink upsert theo PRIMARY KEY: GHI ĐÈ cùng giá trị, KHÔNG cộng dồn
  ⇒ không mất (offset nằm trong checkpoint) + không nhân đôi (upsert idempotent)
```

**Câu nói kèm:**
> Offset Kafka và state nằm chung trong một checkpoint. JDBC sink tự nó chỉ là at-least-once vì không có transaction, nhưng ghi upsert theo khóa chính là idempotent. Nên kết quả cuối là exactly-once về hiệu quả.

---

## Câu hỏi xoáy và câu trả lời ngắn

| Câu hỏi | Trả lời |
|---|---|
| Dùng Kafka thật hay Redpanda? | Redpanda, tương thích Kafka API nên code producer và connector Flink giữ nguyên. Chạy 1 broker trong Docker Compose. |
| Sao key là `pu_location_id`? | Cùng zone vào cùng partition, giữ thứ tự trong zone. Thừa nhận: Flink vẫn shuffle lại theo khóa GROUP BY nên key không giúp bước aggregate; zone đông như Manhattan gây skew. |
| Kafka đảm bảo thứ tự ở mức nào? | Chỉ trong một partition. Vì vậy Flink tính watermark riêng từng partition rồi lấy min, và số event mất thực tế (167) thấp hơn mô phỏng (202). |
| `acks=all` và idempotence để làm gì? | `acks=all`: chờ mọi replica trong ISR ghi xong, không mất. Idempotence: broker dùng producer ID + sequence number để bỏ bản gửi lại, không trùng. Lab chỉ 1 broker nên `acks=all` chưa thể hiện hết tác dụng. |
| Consumer group trong project? | 3 job, 3 group riêng, mỗi job đọc đủ topic. Flink tự lưu offset trong checkpoint; offset commit lên Kafka chỉ để theo dõi lag. |
| Watermark 5 giây mà event trễ 10 phút? | Cố ý để đo được event bị mất. Thực tế chọn theo phân bố độ trễ đo được, ví dụ p99. Watermark dài hơn: mất ít hơn nhưng kết quả ra chậm hơn. |
| Event tới sau khi cửa sổ đóng thì sao? | Job A bỏ. Muốn giữ: allowed lateness hoặc side output (DataStream API), cách GROUP BY + upsert như Job B, hoặc đối soát bằng batch. |
| TTL 2 giờ của Job B có rủi ro gì? | TTL tính theo processing time. Event trễ hơn TTL đến sau khi state bị xóa sẽ đếm lại từ 0 và **ghi đè sai** dòng đúng. Trong lab cả ngày chạy 2.4 phút nên TTL chưa từng kích hoạt. |
| Mini-batch để làm gì? | Gom cập nhật trong 1 giây (tối đa 5,000 bản ghi) rồi chỉ ghi giá trị mới nhất, giảm số lần ghi Postgres; đổi lại chậm thêm khoảng 1 giây. |
| Sao cửa sổ 23:00 không được ghi? | Watermark chỉ tiến khi có event mới. Watermark cuối là 23:59:50, chưa vượt 24:00. |
| Idle-timeout để làm gì? | Partition không có dữ liệu giữ watermark đứng yên, kéo cả job không đóng được cửa sổ nào. Im lặng 30 giây thì bỏ qua partition đó. |
| Sao đặt timezone UTC? | Dữ liệu TLC là giờ New York không kèm múi giờ; producer coi như UTC. Không đặt UTC thì Flink hiển thị theo múi giờ +07:00, 10:00 thành 17:00. |
| Sao chọn Flink mà không phải Spark Streaming? | Flink xử lý từng event, độ trễ thấp, hỗ trợ event time, watermark và state rất mạnh. Spark Structured Streaming chạy micro-batch. |
| Scale lên 10 lần? | Tăng partition và parallelism, xử lý zone nóng bằng salting hoặc aggregate 2 tầng, dùng RocksDB state backend và checkpoint incremental, tăng batch của sink vì Postgres dễ thành nút cổ chai. |
| Làm sao biết kết quả đúng? | `reconcile.py` tính batch trên đúng file đã gửi, so theo từng giờ: Job B khớp 100%, Job A mất đúng phần event trễ mà mô phỏng watermark dự đoán. |

---

## Những điều **không** được nói

- Không nói "Kafka cluster production", "nhiều broker", hay "hàng triệu event mỗi giây": lab là 1 broker Redpanda, 61,843 event.
- Không nói `acks=all` đã chứng minh chống mất dữ liệu khi broker chết: chỉ có 1 broker, chưa thử failover.
- Không nói TTL đã được kiểm chứng: trong lab TTL chưa từng kích hoạt.
- Không nói Job C có khóa chống trùng hoàn hảo: dữ liệu TLC không có trip_id, PK ghép từ các trường nên hai chuyến trùng hết các trường sẽ bị gộp. Thực tế cần event_id từ nguồn.
