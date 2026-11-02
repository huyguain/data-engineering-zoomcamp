# Bài kể project Spark khi phỏng vấn

Dùng cho phần **Đào sâu project** (30'). Mọi con số dưới đây đều là số đo thật trong lab, nguồn ở [day2-spark-hadoop-practice.md](day2-spark-hadoop-practice.md) và [project1-rerun-and-test.md](project1-rerun-and-test.md). Không thêm số nào ngoài các số này.

**Nguyên tắc:** nói rõ ngay câu đầu tiên rằng đây là project tự làm trên dữ liệu public NYC Taxi, cụm YARN là 1 node dựng trên máy cá nhân. Nói trước thì người phỏng vấn đánh giá phần kỹ thuật; để họ tự phát hiện thì mất điểm tin cậy.

---

## Bản 30 giây (khi bị hỏi "kể nhanh project Spark")

> Tôi viết một job PySpark tính doanh thu taxi New York theo **zone × ngày × loại xe** từ khoảng **15.5 triệu chuyến** năm 2021. Job đọc Parquet từ GCS, chạy trên **YARN** với 2 executor, ghi kết quả vào **BigQuery** để làm báo cáo. Điểm tôi tập trung là hiểu job chạy thế nào bên dưới: đo số stage, lượng shuffle, kiểu join, và viết một test đối chiếu để chứng minh output khớp với dữ liệu nguồn tới từng cent.

---

## Bản đầy đủ theo STAR (~3 phút)

### S — Tình huống

> Dữ liệu là chuyến taxi green và yellow của New York, 7 tháng đầu năm 2021: **15,571,166 dòng**, hai nguồn có schema khác nhau (green dùng `lpep_pickup_datetime`, yellow dùng `tpep_pickup_datetime`). Bài toán là cần một bảng tổng hợp hằng ngày cho báo cáo: mỗi zone, mỗi ngày, mỗi loại xe có bao nhiêu chuyến, doanh thu bao nhiêu, quãng đường trung bình bao nhiêu.

### T — Nhiệm vụ

> Tôi cần làm ba việc:
> 1. Viết job Spark có tham số dòng lệnh, chạy được cả local lẫn trên cụm mà không sửa code.
> 2. Đưa job lên **Spark-on-YARN**, đọc dữ liệu từ **GCS** và ghi vào **BigQuery**.
> 3. Chứng minh được kết quả đúng, chứ không chỉ "chạy không lỗi".

### A — Hành động

**1. Gộp hai nguồn và tổng hợp.**

> Tôi đổi tên cột thời gian của hai nguồn về cùng tên, chọn tập cột chung **theo thứ tự cột của một bên** rồi mới union. Lý do: `unionAll` ghép theo vị trí, không theo tên. Lần đầu tôi lấy cột chung bằng `set(a) & set(b)`, thứ tự cột bị xáo ngẫu nhiên, và Spark không báo lỗi vì các cột đều là `double`. Sau đó `groupBy(zone, day, service_type)` để tính count, sum, avg, rồi join với bảng tên zone.

**2. Đọc execution plan và Spark UI để hiểu job.**

> Tôi đo stage bằng REST API của Spark UI. Job có 2 stage chính:
> - **Stage map: 21 task**, vì green có 10 partition đầu vào và yellow có 11. Filter theo ngày được đẩy xuống tầng đọc Parquet (`PushedFilters`), và Spark làm **partial aggregate** ở từng partition. Nhờ vậy 15.5 triệu dòng chỉ còn **6.5 MB** dữ liệu shuffle.
> - **Stage reduce: 6 task.** Plan ghi 200 partition nhưng **AQE** thấy shuffle chỉ có 6.5 MB nên gộp xuống còn 6.
> - **Join là broadcast hash join**, vì bảng zone chỉ có 265 dòng, khoảng 12 KB. Tôi thử tắt broadcast thì job thêm 1 stage và phải shuffle thêm 2.6 MB cho sort-merge join. Tắt cả AQE thì thành 200 + 200 task rất nhỏ.

**3. Đưa lên YARN, GCS và BigQuery.**

> Tôi dựng Hadoop 3.5 pseudo-distributed (1 node, 8 GB, 6 vcore) và submit `--master yarn --deploy-mode client` với 2 executor, mỗi executor 2 core, 2 GB. YARN cấp container 3 GB vì Spark cộng 384 MB overhead rồi YARN làm tròn theo bội 1 GB. Dữ liệu đọc từ `gs://` qua GCS connector, ghi BigQuery qua spark-bigquery connector ở chế độ indirect: ghi Parquet tạm lên bucket rồi chạy một load job.
>
> Trong lúc làm tôi gặp và xử lý mấy lỗi thật:
> - **Job "chạy trên YARN" nhưng thực ra chạy local**: script cũ có `.master("local[*]")` viết cứng trong code, và cấu hình trong code thắng tham số `spark-submit`. Không có lỗi nào báo, chỉ phát hiện vì trang `:8088` không có app mới. Tôi bỏ dòng đó và để master do `spark-submit` quyết định.
> - **Lệch phiên bản GCS connector với Hadoop 3.5**: lỗi `NumberFormatException: "64m"` (connector không hiểu đuôi `m` trong cấu hình mặc định mới) và `NoSuchMethodError` khi Parquet đọc kiểu vectored. Sửa bằng cách ghi đè cấu hình, không cần hạ phiên bản.
> - **Xác thực**: `gcloud auth login` chỉ cho CLI; connector trong JVM cần Application Default Credentials.

**4. Kiểm chứng kết quả.**

> Tôi viết một script đối chiếu tính lại cùng chỉ số theo đường khác, không groupBy và không join, rồi so ba điều: tổng số chuyến trong output bằng số chuyến nguồn (join không làm rơi hay nhân dòng), tổng doanh thu khớp tới cent, và không có hai dòng trùng grain.

### R — Kết quả

> - Bảng `nytaxi.daily_zone_revenue_yarn` trong BigQuery: **91,280 dòng**, 263 zone, tổng **294,750,068.34 USD**. Kết quả giống hệt khi chạy local.
> - Test đối chiếu: **15,562,140 chuyến** ở cả output lẫn nguồn, doanh thu khớp tới cent, **0 dòng trùng grain**.
> - Chạy trên YARN với file local mất **15.8 giây**; đọc từ GCS mất **4 phút 54 giây**. Tôi phân tích ra hai nguyên nhân: đọc qua mạng nhà, không có data locality; và job có 3 action (`count`, `show`, `write`) mà không `cache()`, nên dữ liệu bị đọc lại từ GCS 3 lần.

### Bài học (nói ngắn, chọn 1–2 ý)

> - **Timezone làm đổi kết quả**: cùng job, để timezone mặc định của máy (Việt Nam) ra 91,049 dòng, đặt `America/New_York` ra 91,280 dòng. Giờ tôi luôn đặt `spark.sql.session.timeZone` tường minh.
> - **Đúng kỹ thuật chưa chắc đúng nghiệp vụ**: có một chuyến 0 dặm giá 398,469 USD. Riêng dòng đó chiếm 90% doanh thu cả ngày của zone Midtown Center ngày 18/03. Job không báo gì. Pipeline thật cần tầng kiểm tra chất lượng **trước** khi aggregate.

---

## Câu hỏi xoáy và câu trả lời ngắn

| Câu hỏi | Trả lời |
|---|---|
| Cluster có mấy node? | 1 node pseudo-distributed trên máy cá nhân, 8 GB RAM. Lên nhiều node thì khác ở data locality, shuffle qua mạng thật, và phải tính lại tài nguyên mỗi node. |
| Dữ liệu bao lớn? | 15.5 triệu dòng, khoảng 428 MB Parquet trên GCS. Tôi không gọi đây là big data; mục tiêu là hiểu cơ chế. |
| Shuffle xảy ra ở đâu? | Ở `Exchange` của `groupBy`, đó là ranh giới stage. Join với zones không shuffle vì là broadcast. |
| Vì sao 15 triệu dòng mà shuffle chỉ 6.5 MB? | Partial aggregate ở phía map: mỗi partition chỉ gửi một dòng cho mỗi key, giống `reduceByKey`. |
| Vì sao xin 2g mà YARN cấp 3 GB? | 2,048 + overhead `max(384, 10%)` = 2,432 MB, YARN làm tròn theo `minimum-allocation-mb` 1,024 → 3,072. |
| Vì sao trên `:8088` thấy vCores là 1? | CapacityScheduler mặc định dùng `DefaultResourceCalculator`, chỉ tính RAM. Spark vẫn chạy 2 task song song mỗi executor. |
| Client hay cluster mode? Vì sao? | Client, để xem log ngay khi debug. Production nên dùng cluster mode, vì client mode tắt máy là driver chết, cả job chết. |
| Sao không dùng RDD? CV có ghi RDD. | DataFrame cho mọi biến đổi chính. Tôi đã đo cùng phép tổng: DataFrame 0.3 s, RDD `reduceByKey` 8.5 s, vì RDD trong PySpark phải serialize từng dòng sang Python. RDD tôi dùng để hiểu partition, `reduceByKey` vs `groupByKey`, và `mapPartitions`. |
| Job đọc GCS chậm, tối ưu thế nào? | `cache()` trước các action, bỏ `count`/`show` ở bản production, chạy cụm cùng region với bucket (Dataproc). |
| Nếu dữ liệu lớn gấp 100 lần? | Tăng executor và chỉnh `shuffle.partitions` cho mỗi partition khoảng 100–200 MB (AQE hỗ trợ), kiểm tra skew theo zone trong Spark UI, partition output theo ngày và ghi incremental thay vì ghi đè toàn bộ. |
| Ghi BigQuery thế nào? Cột `day` sao lệch 5 giờ? | spark-bigquery connector, indirect qua bucket tạm. Cột `day` là `TIMESTAMP` nên BigQuery hiển thị UTC: 0 giờ New York là 5 giờ UTC. Nên ghi kiểu `DATE`. |
| Làm sao biết kết quả đúng? | Script đối chiếu: tổng chuyến và doanh thu khớp nguồn, 0 dòng trùng grain. |

---

## Những điều **không** được nói

- Không nói "cụm production", "nhiều node", hay "TB dữ liệu".
- Không nói đã có tầng data quality trong job Spark: mới **phát hiện** dòng rác và **đề xuất** cách xử lý. Nếu bị hỏi, nói cách sẽ làm (lọc `total_amount` âm, `trip_distance = 0` mà giá cao, đưa vào bảng quarantine).
- Không so sánh thời gian chạy local giữa các cấu hình join: ở cỡ dữ liệu này (1–4 giây) không có ý nghĩa. Thứ để so là số stage và lượng shuffle.
