# 📖 Hướng Dẫn Chi Tiết Các Trường Dữ Liệu GDELT GKG v2.1 (Schema Guide)

File này là tài liệu tham khảo chi tiết về ý nghĩa, định dạng và cách ứng dụng của các trường dữ liệu thực tế trong file **GDELT GKG v2.1** dựa trên tài liệu đặc tả kỹ thuật chính thức ([GDELT GKG Codebook V2.1](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf)).

---

## 📊 So Sánh Sự Khác Biệt Giữa Bản Codebook v2.1 Và Schema Local Của Bạn

Khi bạn so sánh cấu trúc file GDELT GKG v2.1 tải về thực tế so với schema local chúng ta dựng:
1. **Cấu trúc 27 cột**: Phiên bản GDELT GKG v2.1 thực tế gồm **27 cột chính** cách nhau bởi dấu TAB.
2. **Cột Tone nằm ở Index 15**: Đây là nơi chứa chuỗi 7 chỉ số cảm xúc phân tách bằng dấu phẩy.
3. **Cột V2Tone nằm ở Index 16**: Cột này lưu trữ thông tin về `V2EnhancedDates` (chứa các ngày tháng nâng cao được nhắc tới trong bài), do đó giá trị thường bị `null` nếu bài viết không có tham chiếu thời gian khác. 
4. **Giải pháp chuẩn hóa**: Khi lập trình local, chúng ta sẽ đổi tên cột ở Index 15 thành `Tone` (hoặc `ToneScoreRaw`) và trích xuất điểm Average Tone từ index này để phân tích.

---

## 📂 Danh Sách Các Cột Cốt Lõi Và Ý Nghĩa Chi Tiết

| Vị trí (Index) | Tên Trường Trong Code | Ý nghĩa thực tế | Định dạng dữ liệu thô | Ứng dụng trong Pipeline |
| :---: | :--- | :--- | :--- | :--- |
| **0** | `RecordID` | ID duy nhất của dòng bản ghi. | `YYYYMMDDHHMMSS-X` | Khóa chính để Join/GroupBy. |
| **1** | `Date` | Thời gian thu thập tin tức. | `YYYYMMDDHHMMSS` | Dùng để phân vùng (Partition) trong BigQuery. |
| **2** | `SourceCollection` | Mã định danh phân loại nguồn. | `1` = Web, `2` = Broadcast | Phân loại nguồn tin tức. |
| **3** | `SourceName` | Tên miền nguồn tin tức. | `nytimes.com`, `bbc.co.uk` | Thống kê mức độ uy tín/số lượng bài viết của nguồn. |
| **4** | `Url` | Link bài báo gốc trực tuyến. | `https://dailynorthwestern.com/...` | Click trực tiếp từ Dashboard để đọc báo. |
| **7** | `Themes` | Danh sách các chủ đề (Themes) thô. | `GENERAL_HEALTH;MEDICAL;` | Dùng hàm `explode()` để lọc chủ đề Kinh tế (`ECON_*`). |
| **8** | `V2Themes` | Chủ đề nâng cao kèm vị trí xuất hiện. | `GENERAL_HEALTH,4516;` | Thích hợp cho xử lý ngôn ngữ học sâu (NLP). |
| **11** | `Persons` | Tên các nhân vật xuất hiện. | `Geoffrey Hinton;Joe Biden` | Đồ thị mạng lưới liên kết nhân vật. |
| **14** | `Organizations` | Danh sách tên doanh nghiệp/tổ chức. | `Federal Reserve;Google` | Phân tích khủng hoảng truyền thông của doanh nghiệp. |
| **15** | `Tone` | **Mấu chốt cảm xúc (V1)** | `Tone, Positive, Negative...` | **Trích xuất số đầu tiên làm Average Tone Score.** |
| **17** | `GCAM` | Hàng nghìn chỉ số cảm xúc nâng cao. | `wc:729,c1.4:2,c12.1:76` | Phục vụ phân tích NLP chuyên sâu (bỏ qua ở bản local). |
| **26** | `XMLExtras` | Siêu dữ liệu bổ sung. | `<PAGE_AUTHORS>...</PAGE_AUTHORS>` | Dùng để cào thêm Tác giả, AMP Url bài viết. |


### 1. `RecordID` (GKGRECORDID)
* **Ý nghĩa**: Mã định danh duy nhất của mỗi dòng bản ghi đồ thị tri thức (GKG Record).
* **Định dạng**: Kiểu chữ (`String`).
* **Ví dụ**: `20260528031500-1` (Bao gồm timestamp của block tin tức + số thứ tự dòng).
* **Ứng dụng**: Làm khóa chính (Primary Key) để phân biệt các bài báo và thực hiện các phép gom nhóm (GroupBy) hoặc ghép nối (Join).

### 2. `Date` (DATE)
* **Ý nghĩa**: Thời gian bài viết được GDELT thu thập và phân tích.
* **Định dạng**: Kiểu chữ (`String`), lưu dưới dạng `YYYYMMDDHHMMSS`.
* **Ví dụ**: `20260528031500` (Nghĩa là ngày 28 tháng 05 năm 2026, lúc 03:15:00 UTC).
* **Ứng dụng**: Rất quan trọng! Lên Cloud chúng ta sẽ ép kiểu cột này thành `Timestamp` để thực hiện **Phân vùng dữ liệu (Partitioning)** trong BigQuery theo ngày.

### 3. `SourceType` (SourceCollectionIdentifier)
* **Ý nghĩa**: Nguồn gốc thu thập của bài báo.
* **Định dạng**: Kiểu chữ đại diện cho số (`String`/`Integer`).
  * `1` = Web (Báo chí điện tử).
  * `2` = Broadcast (Phát thanh/Truyền hình).
* **Ví dụ**: `1`
* **Ứng dụng**: Dùng để lọc nếu bạn chỉ muốn phân tích tin tức trên báo mạng (`SourceType = 1`).

### 4. `SourceName` (SourceCommonName)
* **Ý nghĩa**: Tên miền (Domain) hoặc tên của nguồn xuất bản tin tức.
* **Định dạng**: Kiểu chữ (`String`).
* **Ví dụ**: `nytimes.com`, `bbc.co.uk`, `tuoitre.vn`.
* **Ứng dụng**: Dùng để thống kê xem trang báo nào đăng nhiều tin tức tiêu cực nhất, hoặc trang báo nào có sức ảnh hưởng lớn nhất.

### 5. `Url` (DocumentIdentifier)
* **Ý nghĩa**: Đường dẫn URL trực tiếp liên kết đến bài báo gốc trên internet.
* **Định dạng**: Kiểu chữ (`String`).
* **Ví dụ**: `https://www.nytimes.com/2026/03/24/business/economy/college-graduates-job-market.html`
* **Ứng dụng**: Hiển thị trên Dashboard để người xem click vào đọc bài báo gốc.

### 6. `Counts` / 7. `V2Counts`
* **Ý nghĩa**: Thống kê tần suất xuất hiện của các con số sự kiện cụ thể trong bài báo (ví dụ: số người chết, số tiền thiệt hại).
* **Định dạng**: Chuỗi phức tạp phân tách bằng dấu chấm phẩy `;`.
* **Ứng dụng**: Thường dùng cho các bài toán phân tích thảm họa, dịch bệnh. Dự án Sentiment của chúng ta sẽ bỏ qua cột này để tối ưu hiệu năng.

### 8. `Themes`
* **Ý nghĩa**: Các từ khóa chủ đề (Themes) được AI của GDELT phát hiện trong nội dung bài viết.
* **Định dạng**: Các từ khóa viết hoa ngăn cách bởi dấu chấm phẩy `;`.
* **Ví dụ**: `ECON_INFLATION;TAX_FNCL;UNEMPLOYMENT;GOV_POLICY;`
* **Ứng dụng**: **Cực kỳ quan trọng!** Chúng ta dùng hàm `explode()` để tách chuỗi này ra thành từng dòng độc lập, giúp phân tích cảm xúc của từng ngành hàng, chủ đề kinh tế cụ thể (ví dụ lọc các chủ đề bắt đầu bằng `ECON_`).

### 9. `V2Themes`
* **Ý nghĩa**: Tương tự như `Themes` nhưng có đính kèm thêm vị trí xuất hiện (Character offset) của từ khóa đó trong văn bản gốc.
* **Ví dụ**: `ECON_INFLATION,125;TAX_FNCL,450;` (Nghĩa là từ khóa lạm phát xuất hiện ở ký tự thứ 125).
* **Ứng dụng**: Dùng cho nghiên cứu ngôn ngữ học chuyên sâu. Ở mức độ DE thông thường, ta chỉ cần dùng cột `Themes` ở trên là đủ sạch và nhanh hơn.

### 10. `Locations` / 11. `V2Locations`
* **Ý nghĩa**: Danh sách các địa danh (quốc gia, thành phố) được nhắc đến trong bài viết kèm tọa độ kinh độ/vĩ độ địa lý.
* **Định dạng**: Chuỗi phức tạp chứa tọa độ.
* **Ví dụ**: `1#United States#US#...#38.8951#-77.0364`
* **Ứng dụng**: Dùng để vẽ bản đồ nhiệt cảm xúc toàn cầu (Heatmap) trên Dashboard Looker Studio.

### 12. `Persons` / 13. `V2Persons`
* **Ý nghĩa**: Danh sách tên các nhân vật có sức ảnh hưởng xuất hiện trong bài báo.
* **Ví dụ**: `Joe Biden;Donald Trump;Elon Musk;`
* **Ứng dụng**: Phục vụ bài toán PageRank tìm kiếm nhân vật có sức ảnh hưởng hoặc bị chỉ trích nhiều nhất trong tuần.

### 14. `Organizations`
* **Ý nghĩa**: Danh sách tên các tổ chức, tập đoàn, doanh nghiệp được nhắc đến.
* **Định dạng**: Chuỗi ngăn cách bởi dấu `;`.
* **Ví dụ**: `Federal Reserve;Google;United Nations;`
* **Ứng dụng**: **Quan trọng!** Dùng để lọc xem doanh nghiệp nào đang bị truyền thông réo tên nhiều nhất trong các bài viết tiêu cực (ToneScore < -3).

### 15. `V2Organizations`
* **Ý nghĩa**: Tương tự cột `Organizations` nhưng có thêm vị trí xuất hiện của tổ chức trong bài báo gốc.
* **Ví dụ**: `Federal Reserve,350;Google,780;`

### 16. `Tone`
* **Ý nghĩa**: Chỉ số cảm xúc phiên bản 1 (chứa 6 chỉ số).

### 17. `V2Tone` (Phiên bản nâng cấp của cảm xúc)
* **Ý nghĩa**: **Chứa 7 chỉ số sắc thái cảm xúc** phân tách bằng dấu phẩy `,`.
* **Định dạng**: Chuỗi chứa số thực.
  * Chỉ số số 1: **Average Tone (Điểm cảm xúc trung bình)** từ `-100` (cực kỳ tiêu cực) đến `+100` (cực kỳ tích cực). *Chúng ta sẽ lấy chỉ số này làm điểm Sentiment chính.*
  * Chỉ số số 2: Tỷ lệ từ ngữ tích cực.
  * Chỉ số số 3: Tỷ lệ từ ngữ tiêu cực.
  * Chỉ số số 7: Word Count (Số lượng từ của bài báo).
* **Ví dụ**: `-4.52,1.25,5.77,7.02,23.5,0.0,450`
* **Ứng dụng**: Trích xuất điểm cảm xúc trung bình làm đầu ra cho các phân tích xu hướng.
