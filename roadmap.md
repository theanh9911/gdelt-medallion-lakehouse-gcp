# 🗺️ Lộ Trình Triển Khai & Checklist Dự Án (Chuẩn Medallion Data Pipeline)

File này là **Checklist tương tác** giúp bạn theo dõi tiến độ xây dựng hệ thống dữ liệu lớn của mình qua 3 tầng kiến trúc chuẩn doanh nghiệp: **Bronze (Raw) ➡️ Silver (Cleaned) ➡️ Gold (Curated/Data Warehouse)**.

---

## 📍 CHẶNG 1: Xây Dựng Local Medallion Pipeline bằng PySpark (Tuần 1)
*Mục tiêu: Làm chủ PySpark Core, thiết kế cấu trúc phân tầng dữ liệu thô thành dữ liệu tinh lọc dưới Local.*

- [x] **Bước 1.1**: Thiết lập môi trường ảo `.venv` Python 3.11 độc lập bằng `uv`.
- [x] **Bước 1.2**: Tải dữ liệu GDELT GKG v2.1 thô dạng TAB thực tế về thư mục `data/`.
- [x] **Bước 1.3**: Tải và cấu hình thành công bộ thư viện gốc Hadoop (`winutils.exe`, `hadoop.dll`, `vcores.dll`) dưới Windows để ghi dữ liệu nguyên bản.
- [ ] **Bước 1.4**: Tạo Jupyter Notebook `notebooks/eda_gdelt.ipynb` để thực hành xây dựng:
  * **Tầng Bronze (Raw Lake)**: Đọc file thô theo Schema 27 cột, ghi lưu trữ nguyên bản ra thư mục `data/bronze/` dạng Parquet.
  * **Tầng Silver (Cleaned Lake)**: Đọc từ Bronze, xử lý ép kiểu cột Date sang Timestamp, trích xuất điểm `ToneScore` từ cột `Tone`, lọc trùng lặp và ghi ra thư mục `data/silver/` dạng Parquet.
  * **Tầng Gold (Data Mart)**: Đọc từ Silver, thực hiện kỹ thuật `explode` cột `Themes` để bóc tách chủ đề, lọc các xu hướng kinh tế `ECON_*` và ghi ra `data/gold/` dạng Parquet.
- [ ] **Bước 1.5**: Đóng gói toàn bộ logic 3 tầng này thành một file script hoàn chỉnh [pyspark_eda.py](file:///d:/H/Projects/de/scripts/pyspark_eda.py) chạy mượt mà qua Terminal.

---

## 🐳 CHẶNG 2: Orchestrate Medallion Luồng Local với Airflow (Tuần 2)
*Mục tiêu: Sử dụng Docker và Apache Airflow để tự động hóa việc lập lịch chạy luồng 3 tầng (Bronze -> Silver -> Gold) hằng ngày.*

- [ ] **Bước 2.1**: Dựng môi trường Apache Airflow bằng `docker-compose.yaml` dưới local.
- [ ] **Bước 2.2**: Thiết lập DAG lập lịch hằng ngày điều phối 4 Tasks:
  * **Task 1 (Ingestion)**: Tải file tin tức GDELT mới nhất trong ngày về máy.
  * **Task 2 (Bronze Job)**: Chạy Spark ghi đè dữ liệu thô sang Parquet (Bronze).
  * **Task 3 (Silver Job)**: Chạy Spark làm sạch, lọc trùng và ép kiểu (Silver).
  * **Task 4 (Gold Job)**: Chạy Spark bóc tách chủ đề xu hướng kinh tế (Gold).
- [ ] **Bước 2.3**: Vận hành và debug toàn bộ quy trình tự động hóa thành công trên giao diện Airflow.

---

## ☁️ CHẶNG 3: Thiết Lập Data Lake & DWH trên Google Cloud Platform (Tuần 3)
*Mục tiêu: Di chuyển toàn bộ cấu trúc lưu trữ phân tầng lên GCP sử dụng Cloud Storage và BigQuery.*

- [ ] **Bước 3.1**: Đăng ký tài khoản GCP Free Tier để nhận $300 dùng thử.
- [ ] **Bước 3.2**: Tạo bucket trên **Cloud Storage** đóng vai trò là Cloud Data Lake phân chia làm 3 thư mục: `gs://[data-lake]/bronze/`, `gs://[data-lake]/silver/`, và `gs://[data-lake]/gold/`.
- [ ] **Bước 3.3**: Thiết kế bảng đích `daily_analysis` trên **BigQuery (Data Warehouse)**:
  * Áp dụng **Partitioning** theo ngày (`Date`) để giảm 90% chi phí quét dữ liệu của BI Tool.
  * Áp dụng **Clustering** theo cột Chủ đề (`Theme`) để tăng tốc bộ lọc.

---

## ⚙️ CHẶNG 4: Orchestrate GCP Cloud Pipeline (Tuần 3 - 4)
*Mục tiêu: Đưa Airflow local lên đóng vai trò "nhạc trưởng" tự động tạo và điều khiển cụm tính toán Spark trên Cloud.*

- [ ] **Bước 4.1**: Kết nối an toàn Airflow với tài khoản GCP qua Service Account Key.
- [ ] **Bước 4.2**: Nâng cấp mã nguồn PySpark để đọc trực tiếp dữ liệu thô từ **BigQuery Public Dataset** ở quy mô **hàng triệu bản ghi**, ghi kết quả phân tầng lên GCS Data Lake.
- [ ] **Bước 4.3**: Thiết lập DAG nâng cấp trên Airflow:
  * **Task 1**: Tự động tạo cụm máy chủ Spark Dataproc ảo tạm thời (Ephemeral Cluster) trên GCP.
  * **Task 2**: Gửi PySpark Job chạy luồng Medallion tính toán phân tán quy mô lớn.
  * **Task 3**: Ghi kết quả Gold trực tiếp vào bảng BigQuery của bạn.
  * **Task 4**: Tự động **Xóa cụm Dataproc** ngay lập tức kể cả khi Job lỗi (`trigger_rule="all_done"`) để tiết kiệm chi phí tối đa.

---

## 📊 CHẶNG 5: Trực Quan Hóa & Đóng Gói CV Chuẩn DE (Tuần 4)
*Mục tiêu: Đóng gói dự án để phỏng vấn.*

- [ ] **Bước 5.1**: Kết nối công cụ Looker Studio trực tiếp vào bảng BigQuery để hiển thị biểu đồ.
- [ ] **Bước 5.2**: Viết tài liệu README chuẩn chỉnh trên GitHub mô tả chi tiết tư duy **Kiến trúc Medallion**, thiết kế hạ tầng tối ưu chi phí trên Cloud.
- [ ] **Bước 5.3**: Viết dự án vào CV cá nhân theo mô hình **STAR (Situation - Task - Action - Result)** và chuẩn bị các câu hỏi phỏng vấn DE về tối ưu hóa Spark/BigQuery.
