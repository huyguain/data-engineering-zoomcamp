# Ngày 1 — Thực hành SQL (BigQuery + dbt)

Bảng dùng để thực hành: `de-zoomcamp-500308.nytaxi.yellow_tripdata_partitioned` (yellow taxi **tháng 1–2/2019**, 14.7 triệu dòng, ~2 GB).

> **Quy tắc tiết kiệm chi phí:** mọi query luôn có `WHERE DATE(tpep_pickup_datetime) BETWEEN ...` để BigQuery chỉ đọc các partition cần thiết. Trước khi chạy, xem dòng "This query will process …" ở góc phải editor. Free tier được 1 TB/tháng, cả bộ bài này tốn chưa tới 1 GB.

---

## Phần 1 — Tự đo lại tác dụng của partition và cluster

### Số liệu đo trên chính bảng của bạn (26/10/2026)

| # | Query | Bảng | Số byte quét |
|---|---|---|---|
| A | `SELECT DISTINCT VendorID … WHERE` 1 tuần (01–07/02/2019) | non-partitioned | **235.9 MB** |
| B | Như A | partitioned | **29.3 MB** (giảm ~88%) |
| C | `SELECT DISTINCT VendorID … WHERE` cả tháng 1/2019 | non-partitioned | 235.9 MB |
| D | Như C | partitioned | 123.1 MB (giảm ~48%) |
| E | `SELECT * … LIMIT 10` | non-partitioned | **2,009.3 MB** (đọc toàn bộ bảng dù chỉ lấy 10 dòng) |
| F | `COUNT(*) … WHERE` tháng 1–2 `AND VendorID = 1` | partitioned | 235.9 MB, chạy 692 ms |
| G | Như F | partitioned + clustered | **184.6 MB** (giảm thêm ~22%), chạy 547 ms |

**Bài học rút ra (nói được thành lời):**
- **A → B:** lọc càng hẹp theo cột partition thì càng ít partition phải đọc. 1 tuần trên tổng 2 tháng nên chỉ đọc khoảng 1/8 dữ liệu.
- **C → D:** lọc cả 1 tháng thì chỉ giảm khoảng một nửa. Partition chỉ có lợi khi query lọc một phần nhỏ dữ liệu.
- **E:** `LIMIT` **không** giảm chi phí. `SELECT *` đọc mọi cột.
- **F → G:** câu F lọc cả 2 tháng nên partition không loại được gì. Cluster theo `VendorID` giúp bỏ qua các block không chứa `VendorID = 1`, giảm thêm khoảng 22%.
- **Dry run không ước tính được tác dụng của cluster**, vì việc bỏ block chỉ xảy ra lúc chạy thật. Muốn thấy thì phải chạy thật rồi xem "Bytes billed" trong Job information.

### Cần sửa cho khớp với CV

- Cặp số **1.6 GB → ~106 MB** trong CV lấy từ comment trong [big_query.sql](../../03-data-warehouse/big_query.sql). Đó là số của khóa học, đo trên dữ liệu 2019–2020 với bộ lọc tháng 6/2019. **Bảng của bạn không có dữ liệu tháng 6/2019**, nên các query trong file đó chạy trên bảng của bạn sẽ ra 0 MB.
- Nếu người phỏng vấn hỏi "đo thế nào" mà bạn không tự tái hiện được con số đó thì rất dễ lộ. Có 2 cách xử lý:
  1. **Sửa CV theo số đo của mình**, ví dụ: *"reducing data scanned per query by ~88% (236 MB → 29 MB) through partition pruning, with clustering cutting a further ~22% on vendor-filtered queries"*.
  2. Hoặc nạp thêm dữ liệu 2019–2020 rồi đo lại, cho tới khi tự ra được con số của mình.
- Con số ~90% trong CV đến từ **partition**. Cluster chỉ đóng góp thêm khoảng 22%. Khi trả lời đừng gộp chung hai cái.

### Tự làm

1. Mở BigQuery console, chạy lại câu A và câu B, đọc con số "This query will process" của từng câu.
2. Chạy thật câu F và câu G, mở **Job information**, so sánh "Bytes processed" và "Bytes billed".
3. Chạy query liệt kê partition:
   ```sql
   SELECT partition_id, total_rows
   FROM `de-zoomcamp-500308.nytaxi.INFORMATION_SCHEMA.PARTITIONS`
   WHERE table_name = 'yellow_tripdata_partitioned'
   ORDER BY partition_id;
   ```
   Bạn sẽ thấy có partition từ **2001** đến **2088**. Đây là dữ liệu bẩn, xem tiếp ở bài 6.
4. Tập nói to trong 2 phút: *"Tôi partition theo cột nào, vì sao, đo bằng cách nào, được bao nhiêu."*

---

## Phần 2 — dbt: đi theo luồng model

**Lưu ý:** project dbt của bạn chạy trên **DuckDB** chứ không phải BigQuery (xem `profiles.yml`, `type: duckdb`). Trong khi đó CV ghi *"on top of a BigQuery warehouse"*. Model [fct_daily_zone_revenue.sql](../../04-analytics-engineering/taxi_rides_ny/models/marts/reporting/fct_daily_zone_revenue.sql) có nhánh `{% if target.type == 'bigquery' %}` nên code đã sẵn sàng cho BigQuery. Có 2 hướng:
- Thêm một target `bigquery` vào `profiles.yml` và chạy `dbt build --target bigquery` một lần cho đúng với CV.
- Hoặc sửa CV thành "dbt (DuckDB locally, BigQuery-compatible via cross-database macros)".

**Kết quả lần build gần nhất (19/10/2026):** 9 model, 42 test, 2 seed, tất cả pass.

### Tự làm

1. Mở lần lượt các file theo đúng thứ tự dưới đây, mỗi file trả lời 1 câu: *model này làm gì, grain là gì?*

   | Layer | File | Câu cần trả lời |
   |---|---|---|
   | staging | `stg_yellow_tripdata.sql`, `stg_green_tripdata.sql` | Đổi tên hoặc ép kiểu những cột nào? Vì sao staging phải ứng 1-1 với bảng nguồn? |
   | intermediate | `int_trips_unioned.sql` | Vì sao phải `UNION ALL` mà không dùng `UNION`? Cột `service_type` sinh ra ở đâu? |
   | intermediate | `int_trips.sql` | Thêm logic gì (tạo `trip_id`, loại trùng, join)? |
   | mart | `fct_trips.sql`, `dim_zones.sql`, `dim_vendors.sql` | Grain của fact là gì? Join với dim bằng khóa nào? |
   | reporting | `fct_daily_zone_revenue.sql` | Grain là (zone, ngày, service_type). Vì sao test `unique_combination_of_columns` lại dùng đúng 3 cột đó? |

2. Mở [schema.yml](../../04-analytics-engineering/taxi_rides_ny/models/marts/reporting/schema.yml) và giải thích từng test:
   - `unique_combination_of_columns` (pickup_zone, revenue_day, service_type): đảm bảo grain không bị nhân dòng.
   - `not_null`: cột bắt buộc phải có giá trị.
   - `accepted_values: ['Green', 'Yellow']`: bắt lỗi nếu nguồn sinh ra một loại service lạ.
3. Chạy lại `dbt build` trong venv đã cài `dbt-duckdb`, xem log thứ tự chạy theo DAG. Thử cố tình làm hỏng một test (ví dụ thêm `'Blue'` vào `accepted_values` rồi xóa `'Green'`) để thấy model phía sau bị bỏ qua (SKIP).
4. Chạy `dbt docs generate && dbt docs serve`, mở lineage graph, tự chỉ ra từng layer trên đồ thị.

---

## Phần 3 — Luyện SQL (mỗi bài ≤ 10 phút)

Tự viết trước, rồi mới mở lời giải. Mọi lời giải đều đã được dry run trên bảng của bạn. Số MB ghi bên cạnh là số byte sẽ quét.

### Bài 1 — Top 3 khu vực đón khách có doanh thu cao nhất mỗi ngày (01–07/02/2019)

<details><summary>Lời giải (~44 MB)</summary>

```sql
WITH daily AS (
  SELECT DATE(tpep_pickup_datetime) AS d, PULocationID, SUM(total_amount) AS revenue
  FROM `de-zoomcamp-500308.nytaxi.yellow_tripdata_partitioned`
  WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-02-01' AND '2019-02-07'
  GROUP BY 1, 2
)
SELECT * FROM daily
QUALIFY ROW_NUMBER() OVER (PARTITION BY d ORDER BY revenue DESC) <= 3
ORDER BY d, revenue DESC;
```
- `QUALIFY` lọc theo kết quả window function, giống `HAVING` nhưng dành cho window. Đây là cú pháp của BigQuery. Trên Postgres phải bọc thêm một subquery rồi lọc `WHERE rn <= 3`.
- Câu hỏi mở rộng: nếu 2 khu vực bằng doanh thu nhau thì sao? → Dùng `RANK()` thay cho `ROW_NUMBER()` để giữ cả hai.

</details>

### Bài 2 — Doanh thu mỗi ngày và % thay đổi so với ngày trước

<details><summary>Lời giải (~236 MB)</summary>

```sql
WITH daily AS (
  SELECT DATE(tpep_pickup_datetime) AS d, SUM(total_amount) AS revenue
  FROM `de-zoomcamp-500308.nytaxi.yellow_tripdata_partitioned`
  WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-02-28'
  GROUP BY 1
)
SELECT d, revenue,
  LAG(revenue) OVER (ORDER BY d) AS prev_revenue,
  ROUND(100 * SAFE_DIVIDE(revenue - LAG(revenue) OVER (ORDER BY d),
                          LAG(revenue) OVER (ORDER BY d)), 2) AS pct_change
FROM daily ORDER BY d;
```
- `SAFE_DIVIDE` trả về NULL thay vì báo lỗi khi chia cho 0.
- Dòng đầu tiên có `prev_revenue` là NULL vì không có ngày trước đó.
- Nhìn kết quả: doanh thu cuối tuần có giảm không?

</details>

### Bài 3 — Đếm số chuyến bị trùng trong ngày 01/02/2019

Coi hai chuyến là trùng nếu giống nhau ở: vendor, giờ đón, giờ trả, điểm đón, điểm trả, tổng tiền.

<details><summary>Lời giải (~ vài chục MB)</summary>

```sql
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                   PULocationID, DOLocationID, CAST(total_amount AS STRING)
      ORDER BY tpep_dropoff_datetime DESC) AS rn
  FROM `de-zoomcamp-500308.nytaxi.yellow_tripdata_partitioned`
  WHERE DATE(tpep_pickup_datetime) = '2019-02-01'
)
SELECT COUNTIF(rn > 1) AS duplicate_rows, COUNT(*) AS total_rows FROM ranked;
-- Kết quả thật: 0 dòng trùng / 299,261 dòng
```
- **Bẫy thực tế:** BigQuery **không cho `PARTITION BY` theo cột FLOAT64** (`total_amount`), sẽ báo lỗi *"Partitioning by expressions of type FLOAT64 is not allowed"*. Phải `CAST` sang STRING hoặc nhân 100 rồi ép sang INT64. Lý do: so sánh bằng nhau giữa các số thực không đáng tin.
- Muốn **giữ lại** bản ghi duy nhất thì đổi dòng cuối thành `SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1`.

</details>

### Bài 4 — Doanh thu cộng dồn và trung bình trượt 7 ngày trong tháng 2

<details><summary>Lời giải (~113 MB)</summary>

```sql
WITH daily AS (
  SELECT DATE(tpep_pickup_datetime) AS d, SUM(total_amount) AS revenue
  FROM `de-zoomcamp-500308.nytaxi.yellow_tripdata_partitioned`
  WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-02-01' AND '2019-02-28'
  GROUP BY 1
)
SELECT d, revenue,
  SUM(revenue) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
  AVG(revenue) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)         AS moving_avg_7d
FROM daily ORDER BY d;
```
- `ROWS` tính theo số dòng. `RANGE` tính theo giá trị của cột `ORDER BY`. Nếu có ngày bị thiếu dữ liệu thì "6 dòng trước" không còn là "6 ngày trước".
- 6 ngày đầu tháng chưa đủ 7 ngày, nên giá trị trung bình được tính trên ít dòng hơn.

</details>

### Bài 5 — So sánh `ROW_NUMBER`, `RANK`, `DENSE_RANK` theo số chuyến của từng `payment_type` (ngày 01/02)

<details><summary>Lời giải (~5 MB)</summary>

```sql
WITH t AS (
  SELECT payment_type, COUNT(*) AS trips
  FROM `de-zoomcamp-500308.nytaxi.yellow_tripdata_partitioned`
  WHERE DATE(tpep_pickup_datetime) = '2019-02-01'
  GROUP BY 1
)
SELECT payment_type, trips,
  ROW_NUMBER() OVER (ORDER BY trips DESC) AS row_num,
  RANK()       OVER (ORDER BY trips DESC) AS rnk,
  DENSE_RANK() OVER (ORDER BY trips DESC) AS dense_rnk
FROM t;
```
Không có giá trị bằng nhau thì 3 cột sẽ giống hệt nhau. Hãy tự giải thích vì sao, rồi nêu ví dụ 100, 100, 90 để chỉ ra chỗ khác nhau.

</details>

### Bài 6 — Kiểm tra chất lượng dữ liệu (data quality)

Đếm số dòng: giờ đón nằm ngoài tháng 1–2/2019; giờ trả trước giờ đón; tổng tiền âm; quãng đường bằng 0; số khách NULL hoặc bằng 0.

<details><summary>Lời giải (~589 MB, câu đắt nhất vì quét toàn bảng)</summary>

```sql
SELECT
  COUNTIF(DATE(tpep_pickup_datetime) < '2019-01-01'
       OR DATE(tpep_pickup_datetime) > '2019-02-28')         AS out_of_range_pickup,
  COUNTIF(tpep_dropoff_datetime < tpep_pickup_datetime)       AS dropoff_before_pickup,
  COUNTIF(total_amount < 0)                                   AS negative_amount,
  COUNTIF(trip_distance = 0)                                  AS zero_distance,
  COUNTIF(passenger_count IS NULL OR passenger_count = 0)     AS no_passenger,
  COUNT(*)                                                    AS total_rows
FROM `de-zoomcamp-500308.nytaxi.yellow_tripdata_partitioned`;
```

**Kết quả thật:**

| out_of_range_pickup | dropoff_before_pickup | negative_amount | zero_distance | no_passenger | total_rows |
|---|---|---|---|---|---|
| 784 | 8 | 16,602 | 104,873 | 294,866 | 14,745,987 |

- **Câu chuyện hay để kể khi phỏng vấn:** *"Khi kiểm tra partition, tôi thấy có partition từ năm 2001 đến 2088 trong dữ liệu chỉ thuộc 2 tháng. Đó là lỗi đồng hồ ở thiết bị taxi. Tôi đếm được 784 dòng giờ đón sai, 16.6 nghìn dòng tổng tiền âm (giao dịch hoàn tiền hoặc điều chỉnh), và 105 nghìn chuyến có quãng đường bằng 0. Ở tầng staging tôi lọc bỏ hoặc gắn cờ những dòng này, và thêm dbt test để bắt lỗi."*
- Ở đây `COUNTIF` của BigQuery tương đương `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`.

</details>

### Bài 7 — Pivot: số chuyến theo kiểu thanh toán, mỗi kiểu một cột, theo ngày

<details><summary>Lời giải (~29 MB)</summary>

```sql
SELECT
  DATE(tpep_pickup_datetime) AS d,
  COUNTIF(payment_type = 1)           AS credit_card,
  COUNTIF(payment_type = 2)           AS cash,
  COUNTIF(payment_type NOT IN (1, 2)) AS other
FROM `de-zoomcamp-500308.nytaxi.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-02-01' AND '2019-02-07'
GROUP BY 1 ORDER BY 1;
```
BigQuery còn có toán tử `PIVOT`, nhưng cách dùng `COUNTIF`/`CASE WHEN` chạy được trên mọi database và hay được yêu cầu trong vòng live coding.

</details>

### Câu hỏi lý thuyết nhanh (trả lời miệng, không cần chạy)

- `WHERE` khác `HAVING`, `UNION` khác `UNION ALL`: xem bộ câu hỏi SQL trong [interview-prep-plan.md](interview-prep-plan.md).
- LEFT JOIN làm **nhân số dòng** khi nào? → Khi khóa bên phải không unique. Ví dụ join `trips` với một bảng zone có 2 dòng trùng `LocationID` thì mỗi chuyến bị nhân đôi, và doanh thu bị cộng gấp đôi. Đây là lý do cần test `unique` trên khóa của bảng dim.

---

## Bài nói mẫu 2 phút (checkpoint)

Nói theo **4 ý**: bối cảnh → thiết kế → đo kết quả → dbt. Đừng học thuộc từng chữ, chỉ cần nhớ các con số in đậm.

### Tiếng Việt

> **(1) Bối cảnh, ~15 giây.** Project dùng dữ liệu yellow taxi NYC tháng 1–2/2019, khoảng **14.7 triệu dòng, 2 GB**. Tôi đưa file Parquet lên GCS, tạo external table trong BigQuery, rồi tạo các bảng native để so sánh.
>
> **(2) Thiết kế, ~30 giây.** Trước khi thiết kế, tôi xem query thường dùng thế nào: hầu hết đều **lọc theo khoảng thời gian** và **lọc hoặc group theo vendor**. Vì vậy tôi **partition theo `DATE(tpep_pickup_datetime)`**, mỗi ngày một partition, và **cluster theo `VendorID`**. Partition giúp BigQuery bỏ qua hẳn những ngày không liên quan. Cluster sắp xếp dữ liệu trong mỗi partition để bỏ qua các block không chứa vendor cần tìm.
>
> **(3) Kết quả, ~35 giây.** BigQuery tính tiền theo số byte quét, nên tôi đo bằng chỉ số bytes processed. Một query lọc 1 tuần giảm từ **236 MB xuống 29 MB, khoảng 88%**, nhờ partition pruning. Với query lọc theo vendor trên toàn bộ khoảng thời gian, cluster giảm thêm khoảng **22%** (236 xuống 185 MB). Con số này tôi phải chạy thật mới đo được, vì dry run không ước tính được tác dụng của cluster. Tôi cũng xác nhận `SELECT * LIMIT 10` vẫn quét đủ **2 GB**, nên `LIMIT` không giúp giảm chi phí.
>
> **(4) dbt, ~40 giây.** Phía trên warehouse, tôi tổ chức dbt thành 3 layer. **Staging**: ứng 1-1 với bảng nguồn, đổi tên cột cho thống nhất giữa yellow và green, ép kiểu. **Intermediate**: `UNION ALL` hai nguồn và thêm cột `service_type`, tạo `trip_id` bằng surrogate key, join bảng tra cứu payment type, rồi loại bản ghi trùng bằng `ROW_NUMBER`. **Mart**: `fct_trips` có grain là một chuyến xe, cùng các bảng `dim_zones`, `dim_vendors`, và bảng báo cáo `fct_daily_zone_revenue` có grain là zone, ngày, service type. Tôi có **42 test**, gồm `not_null`, `accepted_values`, và `unique_combination_of_columns` để đảm bảo grain không bị nhân dòng. Test fail thì các model phía sau bị bỏ qua, dữ liệu lỗi không lan xuống tầng báo cáo.

### English version (nếu phỏng vấn bằng tiếng Anh)

> I worked with NYC yellow taxi data for January–February 2019: about **14.7 million rows, 2 GB**, loaded as Parquet into GCS and exposed in BigQuery as an external table, then materialized into native tables.
>
> Most queries filter by a **time range** and filter or group by **vendor**, so I **partitioned by `DATE(tpep_pickup_datetime)`** (daily) and **clustered by `VendorID`**. Partitioning lets BigQuery skip whole days; clustering sorts data inside each partition so it can skip blocks for other vendors.
>
> BigQuery bills by bytes scanned, so I measured bytes processed. A one-week query dropped from **236 MB to 29 MB, about 88%**, thanks to partition pruning. For a vendor-filtered query over the full range, clustering cut another **~22%**. I had to measure that with a real run, because a dry run doesn't account for cluster pruning. I also confirmed that `SELECT * … LIMIT 10` still scans the full **2 GB**.
>
> On top of that I built a dbt project in three layers. **Staging** maps 1-to-1 to the sources and standardizes names and types. **Intermediate** unions yellow and green with a `service_type` column, generates a surrogate `trip_id`, enriches payment types, and deduplicates with `ROW_NUMBER`. **Marts** contain `fct_trips` at one row per trip, `dim_zones` and `dim_vendors`, and a reporting model `fct_daily_zone_revenue` at zone × day × service type. **42 tests** (`not_null`, `accepted_values`, `unique_combination_of_columns` on the grain) guard each layer, and a failing test stops bad data from reaching the reports.

### Câu hỏi nối tiếp thường gặp sau bài nói này

| Câu hỏi | Ý trả lời |
|---|---|
| "Partition theo ngày mà mỗi ngày chỉ khoảng 33 MB. Google khuyến nghị mỗi partition nên khoảng 1 GB, sao vẫn chọn theo ngày?" | Đây là đánh đổi. Với 2 tháng dữ liệu thì partition theo tháng, hoặc chỉ cluster theo ngày giờ đón, có thể hợp hơn. Tôi chọn theo ngày vì query thường lọc theo khoảng ngày, và khi dữ liệu tăng lên nhiều năm thì kích thước mỗi partition sẽ hợp lý hơn. **Nói được đánh đổi này là điểm cộng ở mức Middle.** |
| "Vì sao cluster theo `VendorID` mà không theo `PULocationID`?" | `VendorID` có ít giá trị, nên tác dụng cluster hạn chế (chỉ khoảng 22%). Nếu query hay lọc theo khu vực đón khách thì cluster theo `PULocationID` (khoảng 265 giá trị) sẽ hiệu quả hơn. Cluster tối đa 4 cột, và thứ tự cột quan trọng. |
| "Dữ liệu có vấn đề chất lượng gì không?" | Có partition năm 2001 và 2088 (lỗi đồng hồ thiết bị), 784 dòng giờ đón nằm ngoài khoảng thời gian, 16.6 nghìn dòng tổng tiền âm. Những dòng này được xử lý ở tầng staging và bắt bằng test. |
| "dbt chạy trên warehouse nào?" | Trả lời đúng sự thật: chạy local bằng DuckDB. Model đã có macro rẽ nhánh cho BigQuery (`target.type == 'bigquery'`), nên chuyển sang BigQuery chỉ cần thêm target trong `profiles.yml`. (Nếu đã chạy thử trên BigQuery thì nói như vậy.) |

---

## Checkpoint cuối ngày

- [ ] Tự đo lại được A/B và F/G, nói được "giảm bao nhiêu, nhờ cái gì".
- [ ] Đã chốt cách xử lý con số 1.6 GB → 106 MB trong CV (sửa theo số của mình, hoặc tự tái hiện được).
- [ ] Đi hết luồng dbt từ staging tới reporting, nói được grain của từng model.
- [ ] Làm xong 7 bài SQL, mỗi bài ≤ 10 phút, không mở lời giải.
