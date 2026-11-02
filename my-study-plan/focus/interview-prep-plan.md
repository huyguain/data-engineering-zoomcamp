# Ôn phỏng vấn Junior/Middle Data Engineer — 3 ngày

**Phạm vi: chỉ 4 mảng Data Engineering là SQL, Spark, Kafka, Hadoop**, bám theo những gì CV ghi. Bỏ qua phần backend/fullstack, DevOps, frontend.

### Bản đồ 4 mảng → keyword trong CV → ngày ôn

| Mảng | Keyword trong CV | Ôn ở |
|---|---|---|
| **SQL** | BigQuery (partitioning, clustering, cost optimization), dbt (modeling, tests); Project 1: 1.6 GB → ~106 MB (~90%), dbt staging → intermediate → mart | Ngày 1 |
| **Spark** | PySpark, DataFrames, Spark SQL, RDDs, Spark-on-YARN, BigQuery–Spark integration, GCS; Project 1: join/group/aggregate, local + YARN | Ngày 2 |
| **Hadoop** | HDFS fundamentals, Spark-on-YARN (không có project riêng nhưng CV có ghi, nên **bắt buộc** trả lời được) | Ngày 2 |
| **Kafka** | Kafka, Flink (tumbling windows, late-event handling/upserts); Project 2: Kafka → Flink → PostgreSQL | Ngày 3 |

**Cách ôn từng chủ đề:**
- **Lý thuyết:** Nói được 1 câu định nghĩa, sau đó giải thích bằng ví dụ đời thường, rồi nêu trade-off.
- **Thực hành:** Mở lại đúng file lab, chạy lại và nói được *"trong project tôi làm thế này, vì lý do này"*. Người phỏng vấn sẽ hỏi xoáy vào từng dòng trong CV, nên mỗi con số và mỗi công nghệ trong CV đều phải giải thích được.

---

## NGÀY 1 — SQL (BigQuery + dbt, Project 1)

### Sáng — Lý thuyết

| Khái niệm | Ví dụ đời thường | Ý cần nói được |
|---|---|---|
| **OLTP vs OLAP** | OLTP là quầy thu ngân siêu thị: mỗi lần ghi 1 hóa đơn, phải nhanh và chính xác. OLAP là phòng kế toán cuối tháng: đọc hàng triệu hóa đơn để tổng hợp. | OLTP (Postgres/MySQL) tối ưu ghi từng dòng. OLAP (BigQuery) tối ưu đọc và tổng hợp lượng dữ liệu lớn. |
| **Lưu trữ dạng cột (columnar)** | Cần tính tổng tiền thì chỉ cần rút đúng cột "số tiền" trong sổ, không phải lật từng trang hóa đơn. | Query chỉ đọc các cột cần dùng, nên `SELECT *` vừa đắt vừa chậm. Dữ liệu cùng kiểu đứng cạnh nhau nên nén tốt. |
| **Partition** | Tủ hồ sơ chia ngăn theo tháng: tìm hóa đơn tháng 3 thì mở đúng ngăn tháng 3. | Thường chia theo ngày hoặc giờ. Câu `WHERE` lọc theo cột partition thì BigQuery bỏ qua các ngăn khác (**partition pruning**). Nên dùng cho cột có ít giá trị phân biệt, hay lọc theo khoảng thời gian. |
| **Cluster** | Trong mỗi ngăn, hồ sơ được xếp theo tên khách A→Z, nên tìm "Nguyễn" không phải lật hết. | Dữ liệu trong partition được sắp xếp theo tối đa 4 cột. Hợp với cột có nhiều giá trị phân biệt (vd. `VendorID`, zone) hay dùng để lọc hoặc group. |
| **Partition hay cluster?** | Chia ngăn quá nhỏ (mỗi ngăn 1 tờ) thì riêng việc quản lý ngăn đã tốn công. | Partition quá nhỏ (< ~1 GB) hoặc quá nhiều (giới hạn 4000 partition) thì nên dùng cluster. Khi cần biết trước chi phí một query thì dùng partition, vì cluster không ước tính trước được. |
| **Star schema, fact/dim** | Fact là sổ ghi từng giao dịch (ai mua, lúc nào, bao nhiêu tiền). Dimension là danh bạ khách hàng và danh mục sản phẩm để tra cứu. | Fact chứa số đo và khóa ngoại. Dim chứa thuộc tính mô tả. **Grain** là "một dòng của bảng fact nghĩa là gì", phải chốt grain trước khi thiết kế. |
| **SCD Type 1/2** | Khách chuyển nhà. Type 1: xóa địa chỉ cũ, ghi đè địa chỉ mới. Type 2: giữ cả lịch sử địa chỉ, ghi rõ từ ngày nào đến ngày nào. | Cách làm SCD2: thêm cột `valid_from`, `valid_to`, `is_current`. Trong dbt dùng `snapshot`. |
| **Các layer trong dbt** | Bếp nhà hàng: sơ chế rau (staging) → nấu từng món (intermediate) → bày đĩa phục vụ khách (mart). | Staging: đổi tên cột, ép kiểu, làm sạch, quan hệ 1-1 với bảng nguồn. Intermediate: join/union, logic nghiệp vụ. Mart: bảng fact/dim để BI dùng. |
| **dbt test** | Nhân viên kiểm tra hàng trước khi lên kệ. | Các test có sẵn: `unique`, `not_null`, `relationships`, `accepted_values`. Test fail thì pipeline dừng, dữ liệu lỗi không lan sang bước sau. |
| **ETL vs ELT** | ETL: rửa và thái rau xong mới cất tủ. ELT: cất nguyên liệu thô vào tủ, khi nào nấu mới sơ chế. | Warehouse hiện nay đủ mạnh để transform ngay trong warehouse, nên dùng ELT: load dữ liệu thô vào BigQuery, rồi dbt transform bằng SQL. |
| **BigQuery cost optimization** (CV ghi) | Taxi tính tiền theo km: đi đường ngắn thì trả ít. BigQuery on-demand tính tiền theo **số byte quét**, không theo thời gian chạy. | Chỉ chọn cột cần dùng (không `SELECT *`). Lọc theo cột partition. Cluster theo cột hay lọc. Dùng dry run hoặc ước tính trước khi chạy. `LIMIT` **không** giảm số byte quét. Dữ liệu ngoài nằm trên GCS (external table) thì không ước tính trước được chi phí. Materialize bảng trung gian hay dùng lại. |
| **External vs native table** (lab có dùng) | Đọc sách ở thư viện (external, dữ liệu vẫn nằm trên GCS) so với mua sách về nhà (native, dữ liệu nằm trong BigQuery). | External: không tốn chi phí lưu trữ BigQuery nhưng chậm hơn và không partition/cluster được như native. Lab tạo external table từ GCS rồi copy sang bảng native partitioned. |
### Chiều — Thực hành, gắn với lab

> Bài thực hành chi tiết, kèm số liệu đo trên bảng BigQuery của bạn và 7 bài SQL có lời giải: [day1-sql-practice.md](day1-sql-practice.md)

1. **Chạy lại [big_query.sql](../../03-data-warehouse/big_query.sql)**: so sánh dung lượng "bytes processed" giữa bảng thường và bảng partitioned + clustered để **tự tái hiện con số 1.6 GB → ~106 MB** trong CV.
   - Câu sẽ bị hỏi: *"~90% đó đo bằng cách nào?"* → Xem "This query will process…" trước khi chạy, hoặc dùng dry run.
   - *"Vì sao lại giảm?"* → Nhờ partition pruning theo `tpep_pickup_datetime` và cluster theo `VendorID`.
   - *"Chọn cột partition theo tiêu chí gì?"* → Đó là cột hay xuất hiện trong `WHERE`, và dữ liệu chia ngày/tháng có kích thước đủ lớn.
2. **Mở dbt project [taxi_rides_ny/models](../../04-analytics-engineering/taxi_rides_ny/models/)**, đi theo luồng `stg_yellow_tripdata` / `stg_green_tripdata` → `int_trips_unioned` → `int_trips` → `fct_trips` + `dim_zones` → [fct_daily_zone_revenue.sql](../../04-analytics-engineering/taxi_rides_ny/models/marts/reporting/fct_daily_zone_revenue.sql).
   - Nói được: grain của `fct_trips` là 1 chuyến xe, còn grain của `fct_daily_zone_revenue` là 1 zone trong 1 ngày.
   - Mở [schema.yml](../../04-analytics-engineering/taxi_rides_ny/models/marts/reporting/schema.yml) và giải thích từng test.
   - Chạy `dbt build`.
3. **Luyện SQL trên dataset taxi, mỗi bài ≤ 10 phút:**
   - Top 3 zone doanh thu cao nhất mỗi ngày (`ROW_NUMBER() OVER (PARTITION BY ...)`).
   - Doanh thu tháng và % tăng so với tháng trước (`LAG`).
   - Loại bỏ chuyến trùng, giữ bản ghi mới nhất.
   - Doanh thu cộng dồn (`SUM() OVER (ORDER BY ...)`).
   - Phân biệt `ROW_NUMBER` / `RANK` / `DENSE_RANK`, `WHERE` / `HAVING`, `UNION` / `UNION ALL`, LEFT JOIN làm nhân số dòng khi key không unique.

**Checkpoint:** Nói trôi chảy trong 2 phút: *"Tôi thiết kế bảng partitioned + clustered thế nào, vì sao lại giảm được 90%, và mô hình dbt gồm những layer nào."*

### Câu hỏi phỏng vấn — SQL

`[J]` = Junior hay gặp, `[M]` = Middle hay gặp. Tự trả lời to thành tiếng trước, rồi mới mở gợi ý.

**SQL cơ bản và window function**

1. `[J]` Thứ tự thực thi logic của một câu `SELECT`? Vì sao không dùng được alias của `SELECT` trong `WHERE`?
2. `[J]` `WHERE` khác `HAVING` thế nào?
3. `[J]` Với 3 giá trị 100, 100, 90 thì `ROW_NUMBER`, `RANK`, `DENSE_RANK` trả về gì?
4. `[J]` `UNION` khác `UNION ALL`? Cái nào nhanh hơn?
5. `[J]` `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)` khác nhau thế nào?
6. `[M]` Vì sao `WHERE x NOT IN (SELECT y ...)` có thể trả về 0 dòng?
7. `[M]` LEFT JOIN rồi `WHERE b.col = 'x'` thì có vấn đề gì?
8. `[J]` Viết query loại bản ghi trùng, mỗi `trip_id` chỉ giữ bản ghi mới nhất.

<details><summary>Gợi ý trả lời</summary>

1. `FROM/JOIN → WHERE → GROUP BY → HAVING → SELECT → DISTINCT → ORDER BY → LIMIT`. `WHERE` chạy trước `SELECT` nên lúc đó alias chưa tồn tại. `ORDER BY` thì dùng được alias.
2. `WHERE` lọc từng dòng **trước** khi group. `HAVING` lọc **nhóm** sau khi group, nên dùng được hàm aggregate (`HAVING COUNT(*) > 10`).
3. `ROW_NUMBER`: 1, 2, 3. `RANK`: 1, 1, 3 (có nhảy số). `DENSE_RANK`: 1, 1, 2 (không nhảy số).
4. `UNION` loại bỏ dòng trùng, nên phải sort hoặc hash, chậm hơn. `UNION ALL` giữ nguyên mọi dòng, nhanh hơn. Biết chắc không có trùng thì dùng `UNION ALL`.
5. `COUNT(*)` đếm mọi dòng. `COUNT(col)` bỏ qua dòng có `col` là NULL. `COUNT(DISTINCT col)` đếm số giá trị khác nhau, không tính NULL.
6. Nếu subquery có một giá trị NULL thì `x NOT IN (..., NULL)` cho kết quả UNKNOWN với mọi dòng, nên không dòng nào được trả về. Dùng `NOT EXISTS` hoặc lọc bỏ NULL trong subquery.
7. Các dòng không khớp có `b.col` là NULL, nên bị `WHERE` loại hết, khiến LEFT JOIN thành INNER JOIN. Muốn giữ LEFT JOIN thì đưa điều kiện vào `ON`.
8. Đáp án:
   ```sql
   SELECT * EXCEPT(rn) FROM (
     SELECT *, ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY updated_at DESC) AS rn
     FROM trips
   ) WHERE rn = 1;
   ```
   Trên BigQuery có thể viết gọn bằng `QUALIFY ROW_NUMBER() OVER (...) = 1`.

</details>

**BigQuery và data warehouse**

9. `[J]` OLTP khác OLAP thế nào? Vì sao không chạy báo cáo trực tiếp trên database của ứng dụng?
10. `[J]` Partition khác cluster thế nào? Trong project bạn chọn cột nào, vì sao?
11. `[M]` Query đã có `WHERE` trên cột partition mà vẫn quét toàn bảng. Nguyên nhân có thể là gì?
12. `[J]` Thêm `LIMIT 10` có giảm chi phí BigQuery không?
13. `[M]` External table khác native table thế nào? Khi nào dùng loại nào?
14. `[M]` Kể 3 cách giảm chi phí và tăng tốc query trên BigQuery.

<details><summary>Gợi ý trả lời</summary>

9. OLTP (Postgres/MySQL) lưu theo dòng, tối ưu cho nhiều giao dịch nhỏ đọc/ghi từng dòng. OLAP (BigQuery) lưu theo cột, tối ưu cho việc quét và tổng hợp lượng dữ liệu lớn. Chạy báo cáo nặng trên database của ứng dụng vừa chậm vừa làm ảnh hưởng người dùng thật.
10. Partition chia bảng thành các phần vật lý riêng (theo ngày/tháng hoặc khoảng số nguyên). Query lọc theo cột partition thì chỉ đọc phần liên quan. Cluster sắp xếp dữ liệu bên trong mỗi partition theo tối đa 4 cột, giúp bỏ qua các block không liên quan. Trong project: partition theo `DATE(tpep_pickup_datetime)` vì query hay lọc theo khoảng ngày; cluster theo `VendorID` vì hay lọc và group theo cột này.
11. Một số nguyên nhân: lọc qua giá trị tính động (ví dụ subquery) mà không phải hằng số; bọc cột partition trong một biểu thức BigQuery không tối ưu được; lọc nhầm sang một cột thời gian khác. Phòng ngừa bằng tùy chọn `require_partition_filter = TRUE`.
12. **Không.** BigQuery tính tiền theo số byte của các cột phải quét, mà `LIMIT` được áp dụng sau khi đã quét. Muốn rẻ hơn thì chọn ít cột hơn và lọc theo cột partition.
13. External table: dữ liệu nằm nguyên trên GCS, BigQuery chỉ đọc. Không tốn tiền lưu trữ BigQuery, nhưng chậm hơn và không ước tính trước được chi phí. Native table: dữ liệu được nạp vào BigQuery, nhanh, partition và cluster được. Lab dùng external table để nạp dữ liệu thô, rồi tạo bảng native partitioned từ đó để query.
14. Không dùng `SELECT *`. Lọc theo cột partition và cluster. Lọc dữ liệu sớm trước khi join. Materialize bảng trung gian hay dùng lại. Dùng `APPROX_COUNT_DISTINCT` khi không cần số chính xác. Xem trước số byte sẽ quét (dry run) trước khi chạy.

</details>

**Data modeling và dbt**

15. `[J]` Fact table khác dimension table thế nào? "Grain" là gì?
16. `[M]` SCD Type 1 khác Type 2 thế nào? Cài SCD2 ra sao?
17. `[J]` Trong dbt, staging, intermediate, mart mỗi layer làm gì?
18. `[J]` `ref()` khác `source()` thế nào?
19. `[M]` Các kiểu materialization trong dbt? Incremental model hoạt động ra sao?
20. `[M]` Một dbt test bị fail thì chuyện gì xảy ra? Bạn xử lý thế nào?

<details><summary>Gợi ý trả lời</summary>

15. Fact chứa các sự kiện và số đo (số tiền, quãng đường) cùng khóa ngoại. Dimension chứa thuộc tính mô tả (zone, vendor, ngày). Grain là định nghĩa "một dòng của bảng fact là gì". Ví dụ `fct_trips` có grain là 1 chuyến xe, còn `fct_daily_zone_revenue` có grain là 1 zone trong 1 ngày.
16. Type 1 ghi đè giá trị cũ, không giữ lịch sử. Type 2 thêm dòng mới cho mỗi lần thay đổi, kèm các cột `valid_from`, `valid_to`, `is_current`. Trong dbt cài bằng `snapshot` (chiến lược `timestamp` hoặc `check`).
17. Staging: 1-1 với bảng nguồn, chỉ đổi tên cột, ép kiểu, làm sạch nhẹ. Intermediate: union/join, logic nghiệp vụ (trong project là gộp yellow và green bằng `int_trips_unioned`). Mart: bảng fact/dim cho BI dùng.
18. `source()` trỏ tới bảng thô nằm ngoài dbt, khai báo trong `sources.yml`. `ref()` trỏ tới một model dbt khác. Cả hai giúp dbt dựng được thứ tự chạy (DAG) và tự đổi đúng schema theo môi trường dev/prod.
19. Các kiểu: `view`, `table`, `incremental`, `ephemeral`. Incremental: lần đầu build toàn bộ bảng; các lần sau chỉ xử lý dòng mới, dùng điều kiện trong khối `{% if is_incremental() %}`, và dùng `unique_key` để merge hoặc upsert. Nên lấy lùi thêm vài ngày để bắt dữ liệu đến muộn.
20. `dbt build` chạy model và test theo thứ tự DAG. Test fail thì các model phía sau bị bỏ qua (skip), nên dữ liệu lỗi không lan xuống. Cách xử lý: query thử dòng lỗi (`dbt test --store-failures`), sửa nguồn hoặc logic, hoặc hạ mức nghiêm trọng xuống `warn` nếu được nghiệp vụ chấp nhận.

</details>

---

## NGÀY 2 — Spark + Hadoop (Project 1)

### Sáng — Lý thuyết Spark

| Khái niệm | Ví dụ đời thường | Ý cần nói được |
|---|---|---|
| **Driver / Executor / Cluster Manager** | Bếp trưởng (driver) lên thực đơn và chia việc. Các đầu bếp (executor) nấu. Quản lý nhà hàng (YARN) phân bếp và nhân sự cho từng ca. | Driver tạo kế hoạch (DAG) và chia task. Executor chạy task và giữ dữ liệu cache. Cluster manager (Standalone/YARN/K8s) cấp tài nguyên. |
| **Partition** | Chia 1000 hóa đơn thành 10 xấp cho 10 người đếm cùng lúc. | 1 partition ứng với 1 task, chạy trên 1 core. Quá ít partition thì không chạy song song được. Quá nhiều thì tốn chi phí quản lý. |
| **Lazy evaluation** | Bếp trưởng ghi xong cả thực đơn rồi mới bắt đầu nấu, nhờ vậy gộp được các bước (thái hành một lần cho cả 3 món). | Transformation chỉ được ghi lại, đến khi gặp action (`count`, `write`, `show`) mới chạy. Catalyst tối ưu toàn bộ kế hoạch trước khi chạy. |
| **Narrow vs wide / shuffle** | Narrow: mỗi người tự lọc xấp hóa đơn của mình. Wide: cả phòng phải đổi hóa đơn cho nhau để gom theo quận, tức là chạy qua lại tốn công. | `filter` và `select` là narrow. `groupBy`, `join`, `distinct` là wide, gây ra **shuffle** qua mạng, và **stage bị cắt tại điểm shuffle**. |
| **Job → Stage → Task** | Đơn hàng (job) gồm các công đoạn (stage), mỗi công đoạn chia thành nhiều phần việc giống nhau (task). | Mỗi action tạo 1 job. Shuffle chia job thành các stage. Số task trong một stage bằng số partition. |
| **Spark SQL** (CV ghi) | Cùng một món ăn, gọi bằng tiếng Việt hay tiếng Anh thì bếp vẫn nấu giống nhau. | `df.createOrReplaceTempView("trips")` rồi `spark.sql("SELECT ...")`. SQL và DataFrame API đi qua **cùng Catalyst optimizer**, nên hiệu năng như nhau, chọn cách nào dễ đọc hơn. Khác BigQuery: BigQuery là dịch vụ serverless tự quản lý tài nguyên, còn Spark SQL chạy trên cluster do mình cấp và tune. |
| **RDD vs DataFrame** | RDD giống tự nấu theo từng bước mình viết ra. DataFrame giống gọi món theo thực đơn để bếp trưởng tự tối ưu cách nấu. | DataFrame có schema, được Catalyst và Tungsten tối ưu nên nhanh hơn. RDD dùng khi cần kiểm soát thấp (dữ liệu không có cấu trúc, logic tùy biến). `mapPartitions` giúp khởi tạo tài nguyên đắt (model, kết nối DB) mỗi partition một lần thay vì mỗi dòng một lần. |
| **Broadcast join** | Bảng zone nhỏ như tờ danh bạ: photo cho mỗi đầu bếp một bản, thay vì bắt cả bếp đổi chỗ cho nhau. | Bảng nhỏ (< `autoBroadcastJoinThreshold`, mặc định 10MB) được gửi tới mọi executor, nên tránh được shuffle bảng lớn. Join hai bảng lớn với nhau thì dùng sort-merge join. |
| **Data skew** | Chia việc theo quận, nhưng một quận có số đơn gấp 100 quận khác, nên cả đội ngồi chờ đúng một người. | Dấu hiệu trong Spark UI: một task chạy rất lâu so với các task còn lại. Cách xử lý: salting key, bật AQE skew join, hoặc broadcast join. |
| **`repartition` vs `coalesce`** | `repartition`: trộn lại toàn bộ rồi chia đều. `coalesce`: chỉ gộp vài xấp lại với nhau, không trộn. | `repartition` có shuffle và dùng được để tăng số partition. `coalesce` không shuffle và chỉ dùng để giảm, thường dùng trước khi ghi file để tránh tạo nhiều file nhỏ. |
| **`cache` / `persist`** | Món nước dùng nấu sẵn để dùng cho nhiều món. | Dùng khi một DataFrame được tái sử dụng nhiều lần. Đừng cache bừa vì tốn bộ nhớ. |

### Sáng (tiếp) — Hadoop/HDFS: CV ghi "HDFS fundamentals", không có project riêng

| Khái niệm | Ví dụ đời thường | Ý cần nói được |
|---|---|---|
| **HDFS** | Một cuốn sách dày được xé thành từng chương (block 128MB). Mỗi chương photo ra 3 bản cất ở 3 kho khác nhau. Thủ thư (**NameNode**) giữ mục lục ghi chương nào nằm ở kho nào. Các kho (**DataNode**) giữ nội dung thật. | Replication factor 3 giúp chịu lỗi: mất một máy vẫn còn bản sao. NameNode chỉ giữ metadata, nên nếu nó chết mà không có HA (Standby NameNode) thì cả cụm tê liệt. |
| **Vấn đề nhiều file nhỏ** | Mục lục của thủ thư bị nhồi 1 triệu tờ giấy nhắn nhỏ, đến mức thủ thư quá tải. | Mỗi file và mỗi block tốn RAM trên NameNode, đồng thời mỗi file sinh ra một task. Cách xử lý: gộp file, dùng `coalesce` trước khi ghi, dùng định dạng Parquet/ORC. |
| **Data locality** | Mang đầu bếp tới kho nguyên liệu thay vì chở cả kho về bếp. | Gửi code tới node đang chứa dữ liệu thay vì kéo dữ liệu qua mạng. |
| **YARN** | Phòng nhân sự cấp phòng và người cho từng dự án. **ResourceManager** là trưởng phòng nhân sự. **NodeManager** là quản lý từng tầng. **ApplicationMaster** là trưởng dự án. **Container** là một bàn làm việc gồm CPU và RAM. | Spark trên YARN: driver xin container từ ResourceManager, và executor chạy trong container. Chế độ `client`: driver chạy trên máy submit. Chế độ `cluster`: driver chạy trong cụm, hợp với production. |
| **MapReduce vs Spark** | MapReduce: sau mỗi công đoạn lại cất đồ ăn vào tủ lạnh (ghi đĩa) rồi lấy ra cho công đoạn sau. Spark: giữ đồ ăn trên bàn (RAM) và đi thẳng qua nhiều bước. | MapReduce gồm map → shuffle/sort → reduce, ghi đĩa giữa các bước nên chậm với các tác vụ lặp hoặc nhiều bước. Spark dùng DAG và tính toán trong RAM, nhanh hơn nhiều lần. |
| **Hive** | Lớp phiên dịch: bạn hỏi bằng SQL, Hive dịch thành job chạy trên các file trong HDFS. | **Metastore** lưu schema và vị trí dữ liệu. **Managed table** (drop bảng thì mất luôn dữ liệu) khác **external table** (drop bảng thì chỉ mất metadata). Hive cũng có partition, tương tự BigQuery. Spark đọc và ghi bảng Hive qua metastore. |
| **Hadoop vs Cloud** | Tự xây kho riêng so với đi thuê kho. | HDFS tương ứng GCS/S3, Hive tương ứng BigQuery/Athena, YARN tương ứng Dataproc/EMR. Trên cloud thì lưu trữ và tính toán tách rời nhau. |

### Chiều — Thực hành, gắn với lab

> Bài thực hành chi tiết, kèm số liệu stage/shuffle đo thật và tình trạng YARN/GCS so với CV: [day2-spark-hadoop-practice.md](day2-spark-hadoop-practice.md)

1. **Spark DataFrame:** chạy lại [06_daily_zone_revenue.py](../../06-batch/practice/06_daily_zone_revenue.py) (join + groupBy + aggregate) và [05_spark_sql.py](../../06-batch/practice/05_spark_sql.py).
   - Mở Spark UI (`localhost:4040`), chỉ ra stage nào có shuffle, và xem join là broadcast hay sort-merge (`df.explain()`).
2. **Kiến trúc cluster và RDD:** chạy [07_cluster_anatomy.py](../../06-batch/practice/07_cluster_anatomy.py), [08_rdds.py](../../06-batch/practice/08_rdds.py), [09_map_partitions.py](../../06-batch/practice/09_map_partitions.py).
   - Câu sẽ bị hỏi: *"Tại sao CV ghi dùng cả RDD lẫn DataFrame?"* → Để so sánh cách viết và hiệu năng, và để dùng `mapPartitions` cho logic theo từng batch.
3. **Chạy trên YARN và kết nối GCS/BigQuery:** xem lại [day7-spark-cloud-yarn.html](day7-spark-cloud-yarn.html), chạy lại `spark-submit --master yarn` và giải thích từng tham số `--num-executors`, `--executor-memory`, `--executor-cores`, `--deploy-mode`.
   - *"Đọc từ GCS khác đọc file local ở chỗ nào?"* → Cần GCS connector và credential. Lưu trữ và tính toán tách rời.
   - *"Ghi dữ liệu vào BigQuery thế nào?"* → Spark-BigQuery connector, kèm một bucket tạm để staging dữ liệu.
4. **Hadoop (nếu cụm YARN đã dựng có HDFS):** chạy `hdfs dfs -put`, `hdfs dfs -ls`, `hdfs fsck <file> -files -blocks` để thấy block và replication. Hoặc chỉ cần vẽ lại sơ đồ HDFS + YARN ra giấy.

**Checkpoint:** Vẽ được sơ đồ *Client → YARN RM → AM/Driver → Executors (container) ↔ HDFS/GCS*, và kể được job daily zone revenue chạy qua những stage nào.

### Câu hỏi phỏng vấn — Spark

1. `[J]` Driver và executor làm nhiệm vụ gì?
2. `[J]` Transformation khác action thế nào? Lazy evaluation có lợi gì?
3. `[J]` Narrow transformation khác wide transformation thế nào? Cho ví dụ.
4. `[J]` Job, stage, task là gì? Một stage có bao nhiêu task?
5. `[J]` RDD, DataFrame, Dataset khác nhau thế nào? Vì sao DataFrame thường nhanh hơn RDD?
6. `[J]` Spark SQL khác DataFrame API thế nào về hiệu năng?
7. `[M]` `groupByKey` khác `reduceByKey` thế nào?
8. `[J]` `repartition` khác `coalesce` thế nào?
9. `[M]` `map` khác `mapPartitions` thế nào? Khi nào dùng `mapPartitions`?
10. `[J]` `cache` khác `persist` thế nào? Khi nào nên cache?
11. `[M]` Spark có những kiểu join nào? Khi nào dùng broadcast join?
12. `[M]` Data skew là gì, phát hiện và xử lý ra sao?
13. `[J]` Gọi `collect()` trên một DataFrame lớn thì chuyện gì xảy ra?
14. `[M]` `spark.sql.shuffle.partitions` mặc định là bao nhiêu? Nên chỉnh thế nào?
15. `[M]` AQE (Adaptive Query Execution) làm được những gì?
16. `[M]` Một job Spark chạy chậm, bạn debug theo các bước nào?
17. `[M]` Spark đọc từ GCS và ghi vào BigQuery như thế nào?

<details><summary>Gợi ý trả lời</summary>

1. Driver chạy hàm `main`, tạo SparkSession, dựng DAG, chia stage và task, rồi gửi task cho executor. Executor là process chạy trên các node: chạy task, giữ dữ liệu cache, gửi kết quả về driver.
2. Transformation (`select`, `filter`, `join`) chỉ ghi lại kế hoạch. Action (`count`, `show`, `write`, `collect`) mới thực sự kích hoạt tính toán. Nhờ lazy evaluation, Catalyst nhìn được toàn bộ kế hoạch nên tối ưu được: đẩy bộ lọc xuống sớm (predicate pushdown), chỉ đọc cột cần dùng (column pruning), gộp nhiều bước vào một lần chạy.
3. Narrow: mỗi partition đầu ra chỉ cần đúng 1 partition đầu vào, không cần trao đổi dữ liệu (`filter`, `select`, `map`). Wide: cần dữ liệu từ nhiều partition nên phải **shuffle** qua mạng (`groupBy`, `join`, `distinct`, `orderBy`).
4. Mỗi action tạo 1 job. Job bị cắt thành các stage tại mỗi điểm shuffle. Mỗi stage có số task bằng số partition của stage đó.
5. RDD: API cấp thấp, không có schema, Spark không tối ưu được bên trong. DataFrame: có schema, được tối ưu bởi Catalyst (tối ưu kế hoạch) và Tungsten (quản lý bộ nhớ, sinh code). Dataset: giống DataFrame nhưng có kiểu tĩnh, chỉ có ở Scala/Java. Trong PySpark không có Dataset.
6. Hiệu năng như nhau, vì cả hai đi qua cùng Catalyst và ra cùng execution plan. Chọn cách nào dễ đọc hơn. Trong lab có thể so sánh `explain()` của hai cách viết ([05_spark_sql.py](../../06-batch/practice/05_spark_sql.py)).
7. `reduceByKey` gộp trước ở từng partition rồi mới shuffle (map-side combine), nên lượng dữ liệu shuffle ít. `groupByKey` shuffle toàn bộ giá trị, dễ OOM. Luôn ưu tiên `reduceByKey` hoặc `aggregateByKey`.
8. `repartition(n)` shuffle toàn bộ dữ liệu, tăng hoặc giảm được số partition, chia đều dữ liệu. `coalesce(n)` chỉ gộp các partition sẵn có, không shuffle, chỉ dùng để giảm. Thường dùng `coalesce` trước khi ghi để tránh tạo nhiều file nhỏ.
9. `map` gọi hàm cho từng dòng. `mapPartitions` gọi hàm một lần cho cả partition (nhận vào một iterator). Dùng khi có bước khởi tạo đắt, như mở kết nối DB hoặc load model, để chỉ làm một lần cho mỗi partition ([09_map_partitions.py](../../06-batch/practice/09_map_partitions.py)).
10. `cache()` là `persist()` với mức lưu trữ mặc định: DataFrame dùng `MEMORY_AND_DISK`, RDD dùng `MEMORY_ONLY`. `persist(level)` cho chọn mức lưu trữ. Nên cache khi một DataFrame được dùng lại từ 2 action trở lên. Dùng xong thì gọi `unpersist()`.
11. Các kiểu join: broadcast hash join, sort-merge join (mặc định cho 2 bảng lớn), shuffle hash join. Dùng broadcast khi một bảng nhỏ (dưới `spark.sql.autoBroadcastJoinThreshold`, mặc định 10MB) hoặc dùng hint `broadcast(df)`. Ví dụ trong project: join bảng trips với bảng zone lookup (~265 dòng).
12. Skew là khi một vài key chiếm phần lớn dữ liệu, nên vài task chạy rất lâu trong khi các task khác đã xong. Phát hiện trong Spark UI: trong một stage, thời gian chạy task max lớn hơn rất nhiều so với median. Xử lý: bật AQE skew join; salting (thêm hậu tố ngẫu nhiên vào key nóng, aggregate 2 lần); broadcast bảng nhỏ; lọc riêng key nóng ra để xử lý.
13. Toàn bộ dữ liệu bị kéo về driver, dễ gây OOM ở driver. Nên dùng `show()`, `take(n)`, hoặc ghi ra storage.
14. Mặc định là 200. Dữ liệu nhỏ thì 200 là quá nhiều, sinh nhiều task và file rất nhỏ. Dữ liệu lớn thì 200 quá ít, mỗi partition quá to, dễ spill ra đĩa. Kinh nghiệm: mỗi partition khoảng 100–200MB. Từ Spark 3.x có AQE tự gộp bớt partition.
15. AQE tối ưu lại kế hoạch lúc chạy, dựa trên thống kê thật sau mỗi lần shuffle: gộp các partition shuffle quá nhỏ; đổi sort-merge join sang broadcast join nếu một bên hóa ra nhỏ; tách các partition bị skew trong join. Bật mặc định từ Spark 3.2.
16. Các bước: (1) Spark UI xem stage nào lâu nhất; (2) trong stage đó xem thời gian task có lệch không (skew), dung lượng shuffle read/write, spill, thời gian GC; (3) `df.explain()` xem kiểu join, bộ lọc có được đẩy xuống nguồn không; (4) sửa: lọc sớm và chọn ít cột, broadcast bảng nhỏ, chỉnh số partition, xử lý skew, cache phần dùng lại, tăng executor memory nếu bị spill.
17. Đọc GCS: thêm jar GCS connector, cấu hình service account key, rồi đọc thẳng `spark.read.parquet("gs://bucket/...")`. Ghi BigQuery: dùng spark-bigquery connector, `df.write.format("bigquery").option("temporaryGcsBucket", ...)`. Connector ghi tạm ra GCS rồi load vào BigQuery. Trên Dataproc thì connector có sẵn.

</details>

### Câu hỏi phỏng vấn — Hadoop

1. `[J]` Kiến trúc HDFS gồm những thành phần nào?
2. `[J]` Vì sao block HDFS lớn (128MB) so với block của filesystem thường (4KB)?
3. `[J]` Replication factor là gì? Các bản sao được đặt ở đâu?
4. `[M]` Secondary NameNode có phải bản dự phòng của NameNode không?
5. `[M]` NameNode chết thì sao? HA hoạt động thế nào?
6. `[J]` Vì sao nhiều file nhỏ là vấn đề với HDFS?
7. `[M]` Mô tả luồng ghi và luồng đọc một file trên HDFS.
8. `[J]` HDFS hợp và không hợp với kiểu workload nào?
9. `[J]` YARN gồm những thành phần nào? Submit một job Spark lên YARN thì chuyện gì xảy ra?
10. `[J]` Deploy mode `client` khác `cluster` thế nào?
11. `[J]` MapReduce gồm những pha nào? So với Spark thì sao?
12. `[M]` Data locality là gì?
13. `[J]` Hive: managed table khác external table thế nào?

<details><summary>Gợi ý trả lời</summary>

1. NameNode (master) giữ metadata: cây thư mục, file gồm những block nào, block nằm ở DataNode nào. DataNode (worker) lưu block thật và gửi heartbeat cùng báo cáo block về NameNode định kỳ.
2. Để giảm metadata trên NameNode (ít block hơn thì tốn ít RAM hơn), và để thời gian đọc tuần tự lớn hơn nhiều so với thời gian tìm vị trí trên đĩa, tối ưu cho việc quét dữ liệu lớn. Một block cũng thường ứng với một task, nên block lớn thì ít task hơn.
3. Mỗi block có bao nhiêu bản sao, mặc định là 3. Chính sách mặc định có tính đến rack: bản 1 trên node đang ghi (hoặc một node ngẫu nhiên), bản 2 trên một rack khác, bản 3 cùng rack với bản 2 nhưng khác node. Nhờ vậy chịu được mất một node, thậm chí mất cả một rack.
4. **Không.** Secondary NameNode chỉ định kỳ gộp `fsimage` và `edits log` (checkpoint) để log không phình quá to. Nó không tự lên thay khi NameNode chết.
5. Không có HA thì cả cụm không truy cập được, vì không ai biết block nằm ở đâu. Có HA: một Active NameNode và một Standby NameNode, đồng bộ `edits` qua JournalNodes. ZooKeeper (ZKFC) phát hiện Active chết và tự chuyển Standby lên thay.
6. Mỗi file và mỗi block chiếm khoảng 150 byte RAM trên NameNode, nên hàng triệu file nhỏ làm NameNode quá tải. Ngoài ra mỗi file sinh một task, nên quá nhiều task nhỏ làm job chậm. Xử lý: gộp file (compaction), `coalesce` trước khi ghi, dùng Parquet/ORC, dùng HAR hoặc SequenceFile.
7. **Ghi:** client hỏi NameNode, NameNode chọn danh sách DataNode cho từng block; client ghi vào DataNode đầu tiên, DataNode này chuyển tiếp sang DataNode thứ 2 rồi thứ 3 (pipeline), sau đó ack quay ngược về client. **Đọc:** client hỏi NameNode vị trí các block, rồi đọc trực tiếp từ DataNode gần nhất. Dữ liệu không đi qua NameNode.
8. Hợp: file lớn, ghi một lần đọc nhiều lần, quét tuần tự cho batch. Không hợp: truy cập ngẫu nhiên với độ trễ thấp, sửa dữ liệu giữa file (HDFS chỉ hỗ trợ append), rất nhiều file nhỏ.
9. Thành phần: ResourceManager (cấp tài nguyên cho cả cụm, gồm Scheduler và ApplicationsManager), NodeManager (quản lý container trên từng node), ApplicationMaster (một cái cho mỗi ứng dụng, xin và quản lý container), Container (gói CPU và RAM). Luồng chạy: `spark-submit --master yarn` → RM khởi động AM trong một container → AM (chứa driver nếu chạy chế độ cluster) xin thêm container → NodeManager khởi chạy executor trong các container đó → executor nhận task từ driver.
10. `client`: driver chạy trên máy gõ lệnh submit, hợp để debug và chạy tương tác; tắt máy hoặc đứt mạng thì job chết. `cluster`: driver chạy trong AM bên trong cụm, hợp với production và job lập lịch.
11. Các pha: Map (đọc split, sinh cặp key-value) → Shuffle & Sort (gom theo key) → Reduce (aggregate). Combiner là bước reduce cục bộ trước shuffle. MapReduce ghi đĩa sau mỗi pha. Spark giữ dữ liệu trong RAM và nối nhiều bước trong một DAG, nên nhanh hơn nhiều với tác vụ lặp hoặc nhiều bước, và API cũng gọn hơn.
12. Đưa code tới nơi có dữ liệu thay vì kéo dữ liệu qua mạng. Spark và YARN ưu tiên chạy task trên node đang chứa block. Các mức locality: `PROCESS_LOCAL` → `NODE_LOCAL` → `RACK_LOCAL` → `ANY`. Khi dùng GCS/S3 thì lưu trữ tách khỏi tính toán, nên không còn data locality theo nghĩa này.
13. Managed: Hive quản lý cả metadata lẫn dữ liệu, nên `DROP TABLE` xóa luôn file. External: Hive chỉ quản lý metadata, nên `DROP TABLE` chỉ xóa định nghĩa bảng, file vẫn còn. Dữ liệu dùng chung với hệ thống khác thì nên dùng external table. (Tương tự external table của BigQuery trên GCS.)

</details>

---

## NGÀY 3 — Kafka + Flink (Project 2), và phỏng vấn thử

### Sáng — Lý thuyết

| Khái niệm | Ví dụ đời thường | Ý cần nói được |
|---|---|---|
| **Batch vs Streaming** | Batch: nhận thư cả túi mỗi sáng. Streaming: tin nhắn đến đâu đọc đến đó. | Batch đơn giản, rẻ, có độ trễ (giờ/ngày). Streaming có độ trễ thấp (giây) nhưng phức tạp hơn vì phải xử lý state, thứ tự và dữ liệu đến muộn. |
| **Kafka: topic / partition / offset** | Topic là một kênh tin tức. Partition là các làn đường song song trong kênh. Offset là số thứ tự tin trên mỗi làn, như số trang sách đánh dấu chỗ đang đọc. | Tin được append vào log và **không bị xóa sau khi đọc** (giữ theo retention), nhờ vậy đọc lại được. Thứ tự chỉ được đảm bảo **trong một partition**, và message cùng key luôn vào cùng partition. |
| **Consumer group** | Một nhóm nhân viên chia nhau đọc các làn: mỗi làn chỉ một người đọc. Thêm người thì chia lại (rebalance). | Số consumer hoạt động trong group tối đa bằng số partition. Các group khác nhau đọc độc lập với nhau. |
| **Offset earliest vs latest** | Mới vào nhóm chat: đọc lại từ tin đầu tiên (earliest), hay chỉ đọc tin mới từ bây giờ (latest). | Chỉ áp dụng khi group chưa có offset đã commit. Nếu đã commit thì đọc tiếp từ offset đã commit. |
| **Producer acks / delivery semantics** | Gửi thư thường (at-most-once, có thể thất lạc), gửi bảo đảm và gửi lại nếu không thấy hồi âm (at-least-once, có thể nhận trùng), chuyển phát có mã đơn chống trùng (exactly-once). | `acks=all` kết hợp idempotent producer. Exactly-once cần transaction hoặc checkpoint ở Flink cộng với sink idempotent (upsert). Đây chính là cách làm trong project. |
| **Replication / ISR** | Mỗi làn có bản chính (leader) và bản sao (follower) đặt ở các broker khác nhau. | Leader chết thì một follower nằm trong ISR (in-sync replicas) lên thay. |
| **Event time vs processing time** | Hóa đơn ghi lúc 10:00 (event time) nhưng 10:07 mới được nhập vào máy (processing time). | Phải tổng hợp theo event time thì kết quả mới đúng thực tế. |
| **Tumbling / sliding / session window** | Tumbling: chốt ca mỗi 1 giờ, các ca không chồng lên nhau. Sliding: mỗi 5 phút lại tính doanh thu 1 giờ gần nhất, các khung chồng nhau. Session: một phiên khách ngồi quán, khách im lặng quá 30 phút thì coi là phiên đã kết thúc. | Project dùng tumbling window. Cần nói được khi nào dùng loại nào. |
| **Watermark và late event** | Chốt sổ ca 10h nhưng chờ thêm 5 phút cho các hóa đơn nộp trễ. Nếu sau đó vẫn có hóa đơn trễ thì sửa lại sổ. | Watermark báo cho hệ thống "dữ liệu trước thời điểm T coi như đã về đủ". Event đến muộn nhưng vẫn trong khoảng watermark thì vẫn được tính vào đúng window. Sink **upsert** (`INSERT ... ON CONFLICT DO UPDATE` theo khóa window + key) giúp kết quả phát lại được ghi đè thay vì bị nhân đôi. Event đến sau khi window đã đóng thì mặc định bị bỏ qua. |
| **Checkpoint / state** | Lưu game định kỳ: máy sập thì chơi tiếp từ lần lưu gần nhất. | Flink chụp snapshot state cùng offset Kafka. Khi khôi phục, nó đọc lại từ offset đó. Kết hợp với sink upsert idempotent, kết quả không bị trùng. |
| **Vì sao chọn Flink** | Flink sinh ra cho streaming thật (xử lý từng event). Spark Streaming thì xử lý theo micro-batch. | Độ trễ thấp hơn, xử lý event time và state rất mạnh. |
### Sáng (tiếp) — Thực hành, gắn với lab

Mở [07-streaming/code/src](../../07-streaming/code/src/) và đi lại toàn bộ luồng:
1. [producer.py](../../07-streaming/code/src/producers/producer.py) / [producer_realtime.py](../../07-streaming/code/src/producers/producer_realtime.py): key là gì, serialize thế nào, gửi vào topic nào.
2. [consumer.py](../../07-streaming/code/src/consumers/consumer.py) / [consumer_postgres.py](../../07-streaming/code/src/consumers/consumer_postgres.py): consumer group, `auto_offset_reset`, cách commit offset.
3. [aggregation_job.py](../../07-streaming/code/src/job/aggregation_job.py): tìm đúng dòng định nghĩa **watermark**, **TUMBLE window**, và **sink có PRIMARY KEY** (đây chính là phần upsert).
4. Chạy `docker compose up` và cả pipeline. Thử: tắt consumer rồi bật lại xem offset; gửi một event có timestamp cũ để thấy dòng trong Postgres được **cập nhật** chứ không sinh thêm dòng mới.

Câu sẽ bị hỏi:
- *"Late event được xử lý cụ thể thế nào?"*
- *"Job Flink chết giữa chừng thì có mất hoặc trùng dữ liệu không?"*
- *"Muốn scale lên 10 lần thì sao?"* → Tăng số partition của Kafka, tăng parallelism của Flink, và dùng key phân bố đều để tránh skew.

### Câu hỏi phỏng vấn — Kafka và Flink

**Kafka**

1. `[J]` Topic, partition, offset, broker là gì?
2. `[J]` Kafka đảm bảo thứ tự message ở mức nào?
3. `[J]` Producer chọn partition cho message như thế nào?
4. `[J]` Consumer group là gì? Nếu số consumer nhiều hơn số partition thì sao?
5. `[M]` Rebalance là gì, khi nào xảy ra?
6. `[M]` Auto commit khác manual commit thế nào? Commit trước hay sau khi xử lý message?
7. `[J]` `acks=0`, `acks=1`, `acks=all` khác nhau thế nào?
8. `[M]` At-most-once, at-least-once, exactly-once khác nhau thế nào? Làm sao đạt exactly-once?
9. `[M]` Leader, follower, ISR là gì? Broker chứa leader chết thì sao?
10. `[J]` Kafka khác message queue truyền thống (RabbitMQ) thế nào?
11. `[M]` Retention khác log compaction thế nào?
12. `[M]` Consumer lag là gì, theo dõi để làm gì?
13. `[M]` Tăng số partition của một topic đang chạy thì có rủi ro gì?
14. `[J]` Project của bạn dùng Redpanda. Redpanda khác Kafka thế nào?

<details><summary>Gợi ý trả lời</summary>

1. Topic: kênh logic chứa một loại event (ví dụ `rides`). Partition: topic được chia thành nhiều log append-only để xử lý song song. Offset: số thứ tự của message trong một partition. Broker: một server Kafka, nhiều broker hợp thành một cluster.
2. Chỉ đảm bảo thứ tự **trong một partition**, không đảm bảo giữa các partition. Muốn các event của cùng một đối tượng (ví dụ cùng một tài khoản) giữ đúng thứ tự thì dùng đối tượng đó làm key.
3. Có key thì dùng `hash(key) % số_partition`, nên cùng key luôn vào cùng partition. Không có key thì dùng sticky partitioner: gom một batch vào một partition rồi mới đổi sang partition khác. Cũng có thể tự viết partitioner.
4. Một nhóm consumer cùng chia nhau đọc một topic. Mỗi partition chỉ được giao cho 1 consumer trong group. Consumer thừa (nhiều hơn số partition) sẽ ngồi không. Các group khác nhau đọc độc lập, mỗi group tự giữ offset riêng.
5. Rebalance là việc chia lại partition giữa các consumer khi có consumer vào hoặc ra khỏi group, consumer bị coi là chết (không gửi heartbeat, hoặc xử lý quá lâu vượt `max.poll.interval.ms`), hoặc topic thêm partition. Trong lúc rebalance, việc đọc bị gián đoạn.
6. Auto commit: commit theo chu kỳ, nên có thể commit trước khi xử lý xong (mất message) hoặc xử lý xong mà chưa kịp commit (đọc trùng). Manual commit **sau khi xử lý xong** cho at-least-once, nên phía ghi kết quả phải idempotent để chịu được message trùng.
7. `acks=0`: không chờ xác nhận, nhanh nhất nhưng có thể mất message. `acks=1`: chờ leader ghi xong, mất message nếu leader chết trước khi follower kịp sao chép. `acks=all`: chờ tất cả replica trong ISR ghi xong, an toàn nhất. Thường dùng kèm `min.insync.replicas=2`.
8. At-most-once: không bao giờ trùng nhưng có thể mất. At-least-once: không mất nhưng có thể trùng. Exactly-once: idempotent producer (`enable.idempotence=true`) kết hợp transaction để đọc-xử lý-ghi trong Kafka; hoặc, như trong project, checkpoint của Flink lưu cùng offset Kafka và sink idempotent (upsert theo primary key). Chạy lại thì kết quả bị ghi đè chứ không bị nhân đôi.
9. Mỗi partition có 1 leader nhận mọi lượt ghi và đọc, các follower sao chép dữ liệu từ leader. ISR (in-sync replicas) là các replica đang theo kịp leader. Leader chết thì controller bầu một replica trong ISR lên làm leader mới.
10. RabbitMQ: message bị xóa sau khi được xử lý xong, broker đẩy message tới consumer, có routing linh hoạt. Kafka: lưu log bền vững theo thời gian retention, consumer tự kéo message về và tự giữ offset, đọc lại được (replay), nhiều group cùng đọc một luồng, lưu lượng rất cao. Kafka hợp làm "xương sống" cho luồng event và pipeline dữ liệu.
11. Retention xóa dữ liệu theo thời gian hoặc dung lượng (ví dụ giữ 7 ngày). Compaction giữ lại **giá trị mới nhất của mỗi key**, hợp với dữ liệu kiểu trạng thái (ví dụ số dư hiện tại của tài khoản, changelog).
12. Lag là khoảng cách giữa offset mới nhất của partition và offset consumer đã commit. Lag tăng dần nghĩa là consumer xử lý không kịp, cần scale consumer, tăng partition, hoặc tối ưu phần xử lý. Đây là metric quan trọng nhất để giám sát.
13. Ánh xạ từ key sang partition thay đổi (vì `hash % n` đổi), nên thứ tự theo key bị phá vỡ trong giai đoạn chuyển. Số partition chỉ tăng được, không giảm được. Vì vậy nên ước lượng số partition từ đầu.
14. Redpanda tương thích với Kafka API (code producer/consumer giữ nguyên), viết bằng C++, không cần JVM và ZooKeeper, dễ chạy local. Các khái niệm topic, partition, offset, consumer group đều giống Kafka.

</details>

**Flink và streaming**

15. `[J]` Batch processing khác stream processing thế nào?
16. `[J]` Event time khác processing time thế nào? Project dùng loại nào?
17. `[J]` Tumbling, sliding, session window khác nhau thế nào? Cho ví dụ dùng từng loại.
18. `[M]` Watermark là gì? Trong project bạn đặt watermark bao nhiêu, vì sao?
19. `[M]` Late event được xử lý thế nào trong project? Event đến **sau** watermark thì sao?
20. `[M]` Vì sao bảng sink trong Postgres cần PRIMARY KEY?
21. `[M]` Checkpoint là gì? Job Flink chết giữa chừng thì có mất hoặc trùng dữ liệu không?
22. `[J]` Vì sao chọn Flink mà không dùng Spark Structured Streaming?
23. `[J]` `scan.startup.mode = earliest-offset` trong project có ý nghĩa gì?

<details><summary>Gợi ý trả lời</summary>

15. Batch xử lý một khối dữ liệu có giới hạn theo lịch (mỗi giờ hoặc mỗi ngày): đơn giản, rẻ, độ trễ cao. Stream xử lý dữ liệu không giới hạn ngay khi tới: độ trễ thấp (giây), nhưng phải xử lý state, thứ tự, dữ liệu đến muộn và lỗi.
16. Event time là thời điểm sự kiện thật sự xảy ra (`tpep_pickup_datetime`). Processing time là thời điểm hệ thống xử lý event. Project dùng **event time** (`event_timestamp` tính từ `tpep_pickup_datetime`), nên kết quả đúng kể cả khi event đến trễ hoặc khi chạy lại dữ liệu cũ.
17. Tumbling: các khung cố định, không chồng nhau (doanh thu từng giờ, đây là cách project dùng: `TUMBLE ... INTERVAL '1' HOUR`). Sliding (hop): khung cố định nhưng trượt và chồng lên nhau (doanh thu 1 giờ gần nhất, cập nhật mỗi 5 phút). Session: khung theo hoạt động, tự đóng sau một khoảng không có event (phiên sử dụng của một người dùng).
18. Watermark là mốc báo "các event có thời gian trước T coi như đã về đủ", dùng để quyết định lúc đóng window. Project đặt `event_timestamp - INTERVAL '5' SECOND`, tức chấp nhận event trễ tối đa 5 giây. Đây là đánh đổi: chờ lâu hơn thì kết quả đầy đủ hơn nhưng độ trễ cao hơn.
19. Event trễ nhưng vẫn trong giới hạn watermark thì vẫn được tính vào đúng window theo event time. Flink phát kết quả khi watermark vượt qua thời điểm cuối window, và nếu kết quả của một window được phát lại thì sink upsert sẽ **ghi đè** dòng cũ trong Postgres thay vì thêm dòng mới. Event đến **sau** khi window đã đóng thì mặc định bị **bỏ qua**. Muốn giữ lại thì dùng allowed lateness hoặc side output (DataStream API), hoặc đối soát lại bằng một job batch. Nói được ý này là điểm cộng ở mức Middle. Số đo thật trong project ([07-streaming/practice](../../07-streaming/practice/README.md)): chèn 1.230 event trễ 10 phút vào 61.843 chuyến, job `TUMBLE` bỏ mất 167 event (đúng những event trễ vắt qua mốc giờ). Job `GROUP BY` theo giờ cộng upsert thì khớp batch 100%, đổi lại phải giữ state và đặt TTL.
20. `PRIMARY KEY (window_start, PULocationID)` làm JDBC sink chạy ở chế độ upsert (`INSERT ... ON CONFLICT DO UPDATE`). Khi Flink phát lại kết quả, do cập nhật hoặc do khôi phục từ checkpoint, dòng cũ được cập nhật chứ không sinh bản trùng. Đây là điểm mấu chốt để ghi idempotent.
21. Checkpoint định kỳ chụp state của các operator cùng offset Kafka (project đặt `enable_checkpointing(10 * 1000)`, tức mỗi 10 giây). Khi job chết, Flink khôi phục state từ checkpoint gần nhất và đọc lại Kafka từ offset đã lưu. Kết quả nội bộ không mất và không trùng. Ở phía sink, một số kết quả có thể được ghi lại lần nữa, nhưng nhờ upsert nên không bị nhân đôi.
22. Flink xử lý từng event (true streaming) nên độ trễ thấp hơn, hỗ trợ event time, watermark và state rất mạnh. Spark Structured Streaming chạy theo micro-batch, hợp khi team đã dùng Spark và chấp nhận độ trễ vài giây. Thay vì nói "Flink tốt hơn", hãy nêu đúng tiêu chí chọn: độ trễ, độ phức tạp của state, công nghệ team đang dùng.
23. Khi khởi động, job đọc topic từ offset đầu tiên, nên xử lý lại toàn bộ dữ liệu đang có trong topic. Hợp cho demo và cho backfill. Trong production, khi khôi phục từ checkpoint thì Flink dùng offset trong checkpoint, không dùng giá trị này.

</details>

### Chiều — Phỏng vấn thử 90 phút và vá lỗ hổng

| Phần | Thời gian | Nội dung |
|---|---|---|
| Giới thiệu | 5' | 1 phút tóm tắt 2 project DE và 4 mảng SQL, Spark, Kafka, Hadoop. |
| Đào sâu project | 30' | Kể 2 project theo khung tình huống – nhiệm vụ – hành động – kết quả, sau đó trả lời câu hỏi xoáy vào từng dòng CV. |
| SQL live | 20' | 2 bài window function trên dữ liệu taxi. |
| Lý thuyết | 20' | Spark, Hadoop, Kafka: hỏi ngẫu nhiên các câu trong bảng của 3 ngày. |
| Thiết kế hệ thống | 15' | *"Thiết kế pipeline giao dịch ngân hàng: real-time phát hiện bất thường, và cuối ngày báo cáo."* → Gộp cả 2 project: Kafka → Flink (cảnh báo) cùng với Kafka → GCS/HDFS → Spark → BigQuery/Hive → dbt mart. |

Cuối ngày: ôn lại các câu bị bí, rồi đọc to một lượt cả 2 phần kể project.

---

## Bảng tra nhanh: câu xoáy theo từng dòng CV

| Dòng trong CV | Câu hỏi gần như chắc chắn gặp |
|---|---|
| Partitioned + clustered, ~90% | Partition theo cột nào, cluster theo cột nào, vì sao? Đo bằng cách nào? Khi nào partition làm chậm thay vì nhanh hơn? |
| Spark DataFrame + RDD | Khác nhau thế nào? Khi nào dùng RDD? Shuffle xảy ra ở đâu trong job của bạn? |
| Spark-on-YARN | Vai trò của RM/NM/AM? Chế độ client hay cluster? Cấp bao nhiêu executor/memory, vì sao? |
| Spark ↔ GCS/BigQuery | Dùng connector nào? Xác thực ra sao? Vì sao không dùng HDFS? |
| dbt staging → mart, tests | Mỗi layer làm gì? Grain của mart? Test fail thì xử lý thế nào? Incremental model? |
| Kafka → Flink → Postgres | Thứ tự message? Delivery semantics? Consumer group? |
| Tumbling window + late event upsert | Watermark là gì? Sau khi window đã đóng, event trễ đi đâu? Khóa upsert là gì? |
| Skills: HDFS fundamentals | HDFS block/replication? NameNode chết thì sao? Vì sao nhiều file nhỏ là vấn đề? MapReduce vs Spark? (Hive: chỉ cần biết mức khái niệm, CV không ghi.) |
| Skills: Spark SQL | Spark SQL khác DataFrame API thế nào? Khác BigQuery SQL thế nào? |
| Skills: BigQuery cost optimization | BigQuery tính tiền thế nào? `LIMIT` có giảm chi phí không? External table vs native table? |

## Chuẩn bị câu trả lời cho các điểm dễ bị hỏi vặn trong CV

| Điểm trong CV | Người phỏng vấn có thể hỏi | Hướng trả lời |
|---|---|---|
| "6/2025 – Present, Data Engineer" nhưng không ghi công ty | "Bạn làm Data Engineer ở công ty nào, team mấy người?" | Nói thẳng: đây là 2 project tự xây dựng trong quá trình chuyển sang DE (theo chương trình Data Engineering Zoomcamp, dữ liệu public), song song với công việc fullstack ở Eastgate. Nói thật ngay từ đầu tốt hơn nhiều so với để người phỏng vấn tự phát hiện, sau đó chuyển ngay sang chi tiết kỹ thuật của project. |
| "multi-GB", "large-scale trip data" | "Dữ liệu cụ thể bao nhiêu dòng, bao nhiêu GB? Cluster có bao nhiêu node?" | Chuẩn bị **số liệu thật** từ lab trước khi đi: số dòng của bảng yellow/green, dung lượng, thời gian job Spark chạy, cấu hình YARN (số node, executor, memory). Không phóng đại. |
| "Spark-on-YARN cluster" | "Cluster đó dựng ở đâu, mấy node?" | Mô tả đúng môi trường đã dựng theo `06-batch/setup/hadoop-yarn.md`. Nếu là cụm 1 node hoặc dựng trong Docker thì nói rõ, rồi giải thích lên nhiều node thì khác ở chỗ nào. |
| "~90% (1.6 GB → ~106 MB)" | "Đo bằng cách nào? Query đó lọc gì?" | Có sẵn câu query cụ thể trong [big_query.sql](../../03-data-warehouse/big_query.sql) (lọc theo khoảng ngày + `VendorID`) và con số "bytes processed" trước/sau. |