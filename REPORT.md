# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Huy Hưng  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  run 1/3 … 28.9s
  run 2/3 … 23.6s
  run 3/3 … 23.5s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Mỗi lần retry pipeline, `gold_training_set` tăng thêm hàng và một `ticket_id` xuất hiện nhiều lần. |
| **Nguyên nhân** | Model incremental không có `unique_key`, nên dbt ghi bằng phép append. Retry cùng ngày vì thế chèn lại dữ liệu cũ. Do nguồn CDC có update ở ngày khác ngày tạo, xoá theo partition ngày cũng không bảo vệ đúng grain entity. |
| **Cách khắc phục** | Trong `gold_training_set.sql`, dùng `unique_key='ticket_id'` và chiến lược `merge`. Trong DAG, đặt `catchup=False`, `max_active_runs=1` để tránh backfill và các run ghi đồng thời. Hai tham số DAG giảm khả năng kích hoạt; phép merge mới xử lý root cause. |
| **Bằng chứng** | Sau sửa: 12.480 hàng, không lặp ticket; checksum ba lượt đều `8dd7c98653`. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng chỉ có 8.645 thay vì 9.100 cặp `(event_date, customer_id)`; các cặp thiếu nằm ở ngày quá khứ. |
| **P99 độ trễ đo được** | **2,725833 ngày** (khoảng 65,42 giờ) |
| **Lookback đã chọn** | 3 ngày — làm tròn lên từ P99 và đồng thời bao phủ max đo được là 2,944688 ngày. |
| **Nguyên nhân** | Điều kiện `event_date > max(event_date)` chỉ nhận ngày sự kiện mới hơn ngày lớn nhất đã có. Event xảy ra ở ngày cũ nhưng tới kho muộn không bao giờ được tính ở các lượt sau. |
| **Cách khắc phục** | Tính lại cửa sổ từ `max(event_date) - interval 3 day`, đồng thời dùng khóa ghép `['event_date', 'customer_id']` và `merge` để kết quả tính lại thay thế hàng cũ. |
| **Bằng chứng** | Trước: 8.645 hàng; sau: 9.100 hàng; checksum ba lượt đều `3db448685c`. Tỷ lệ event trễ hơn một ngày là 5,0509%. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 bao phủ gần như toàn bộ dữ liệu trễ mà không để một ngoại lệ cực đoan làm
> cửa sổ tăng vô hạn. Window càng dài thì mỗi lượt incremental càng phải đọc,
> aggregate và merge nhiều partition cũ. Với bộ dữ liệu đo được, 3 ngày cũng
> vượt max; trong production cần theo dõi phần đuôi và backfill riêng các ngoại
> lệ ngoài SLA.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Pipeline vẫn chạy sau khi backend đổi priority từ số sang nhãn, nhưng Silver có 6.606 giá trị NULL hoặc ngoài miền 1..4. |
| **Nguyên nhân** | `try_cast` coi các nhãn hợp lệ là NULL nhưng lại chấp nhận các số ngoài miền như 0, 5 và -1. Contract bị tắt và không có test miền giá trị nên lỗi âm thầm đi tới model. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | `'1'..'4'`: giữ nguyên; `urgent/high/medium/low`: map thành `1/2/3/4`; giá trị còn lại: trả NULL và đưa đúng bản ghi CDC vào quarantine. |
| **Cách khắc phục** | Chuẩn hóa qua một macro dùng chung; lọc bản ghi lỗi trước khi `row_number` để ticket vẫn dùng trạng thái hợp lệ gần nhất; bật contract, thêm `not_null` và `accepted_values`; định tuyến lỗi sang quarantine. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng; Silver vẫn đủ 12.480 ticket và priority sạch; `dbt test` 11/11 pass. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Bronze nên lưu payload thô để không mất bằng chứng phục vụ điều tra và replay.
> Việc chuẩn hóa và phân luồng lỗi thuộc Silver. Không nên để 312 bản ghi lỗi
> chặn hơn 130.000 event và 31.200 chunk hợp lệ; pipeline tiếp tục phục vụ dữ
> liệu tốt, còn quarantine trở thành hàng đợi có thể quan sát và xử lý lại.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A và B |
| **Nguyên nhân — A** | Dataset có 5.000 file Parquet rất nhỏ, không partition theo cột ngày mà dashboard lọc; điều kiện `strftime(event_time, ...)` bọc cột trong hàm nên không hỗ trợ pruning hiệu quả. Engine phải mở toàn bộ file và quét 5.000.000 đơn vị công việc dù dữ liệu thật chỉ có 130.683 hàng. |
| **Cách khắc phục — A** | Compact thành dataset partition theo `event_date` (14 partition), sắp hàng theo `event_date, customer_name`, dùng row group 2.048; query bật Hive partitioning và lọc trực tiếp `event_date = date '2026-08-09'`. |
| **Bằng chứng — A** | File: 5.000 → 14; rows scanned: 5.000.000 → 9.324, giảm **536,3×**; rows on disk vẫn 130.683; result hash giữ nguyên `4379e4c5d9f3`. |
| **Nguyên nhân — B** | Consumer commit offset trước khi ghi. Nếu chết sau commit, restart bỏ qua batch chưa ghi và gây at-most-once/mất dữ liệu. Chỉ đảo sang ghi trước–commit sau tạo at-least-once nhưng batch có thể được replay và trùng nếu phép ghi không idempotent. |
| **Cách khắc phục — B** | Ghi batch trước rồi mới commit offset; đặt `event_id` làm primary key và dùng `ON CONFLICT DO UPDATE`. `DO UPDATE` giữ được nội dung mới nhất nếu message replay đã đổi, trong khi `DO NOTHING` sẽ giữ phiên bản cũ. |
| **Bằng chứng — B** | Crash ở batch 7 khi offset là 3.000; restart đọc lại phần chưa commit. Kết quả C = A = 20.000 hàng, 20.000 `event_id`, không mất và không trùng; `make crash-test` báo ĐẠT. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra grain, natural key và semantics ghi khi scheduler retry. |
| 2 | So sánh event time với ingestion time và đo percentile độ trễ trước khi chọn lookback. |
| 3 | Kiểm tra contract, phân bố giá trị và đường đi của bản ghi không hợp lệ. |
