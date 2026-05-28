terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ==========================================
# 1. KÍCH HOẠT TỰ ĐỘNG CÁC API TRÊN GCP
# ==========================================
resource "google_project_service" "gcp_services" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "dataproc.googleapis.com",
    "compute.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

# ==========================================
# 2. KHỞI TẠO CÁC GCS BUCKETS (DATA LAKE)
# ==========================================

# Bucket 1: Data Lake chứa dữ liệu phân tầng (Bronze, Silver, Gold)
resource "google_storage_bucket" "data_lake" {
  name          = var.data_lake_bucket_name
  location      = var.region
  force_destroy = true # Cho phép xóa nhanh khi cần dọn dẹp dự án

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # Tự động xóa dữ liệu thô sau 30 ngày để tiết kiệm chi phí
    }
  }

  depends_on = [google_project_service.gcp_services]
}

# Bucket 2: Nơi lưu trữ script PySpark
resource "google_storage_bucket" "code_scripts" {
  name          = var.code_scripts_bucket_name
  location      = var.region
  force_destroy = true

  depends_on = [google_project_service.gcp_services]
}

# ==========================================
# 3. THIẾT LẬP DATA WAREHOUSE (BIGQUERY)
# ==========================================

# Khởi tạo Dataset media_sentiment
resource "google_bigquery_dataset" "media_sentiment_dataset" {
  dataset_id                  = "media_sentiment"
  friendly_name               = "Media Sentiment Analysis"
  description                 = "Dataset chứa kết quả phân tích xu hướng và cảm xúc từ GDELT"
  location                    = var.region
  default_table_expiration_ms = 3600000 * 24 * 90 # Tự hủy bảng nháp sau 90 ngày

  depends_on = [google_project_service.gcp_services]
}

# Khởi tạo bảng đích daily_analysis tối ưu hóa (Partitioned & Clustered)
resource "google_bigquery_table" "daily_analysis_table" {
  dataset_id = google_bigquery_dataset.media_sentiment_dataset.dataset_id
  table_id   = "daily_analysis"

  # CỦNG CỐ KHUYÊN DÙNG LATEST:
  # Tắt tính năng chống xóa bảng mặc định để bạn dễ dàng chạy lệnh "terraform destroy" dọn dẹp local
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "DATE" # Phân vùng bảng theo ngày của cột DATE
  }

  clustering = ["Theme"] # Cụm dữ liệu vật lý theo nhóm Chủ đề

  # LATEST BEST PRACTICE: Sử dụng hàm jsonencode() của HCL thay thế cho chuỗi EOF
  # Giúp code cực kỳ sạch, không bị lỗi cú pháp thụt lề và tự động escape ký tự
  schema = jsonencode([
    {
      "name": "GKGRECORDID",
      "type": "STRING",
      "mode": "REQUIRED",
      "description": "ID duy nhất của bản ghi GDELT"
    },
    {
      "name": "DATE",
      "type": "TIMESTAMP",
      "mode": "REQUIRED",
      "description": "Thời gian xuất bản bài viết"
    },
    {
      "name": "SourceName",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Tên nguồn tin tức"
    },
    {
      "name": "Url",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Link bài báo gốc"
    },
    {
      "name": "Theme",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Chủ đề kinh tế vĩ mô"
    },
    {
      "name": "ToneScore",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Điểm cảm xúc Sentiment Score trung bình"
    }
  ])

  depends_on = [google_bigquery_dataset.media_sentiment_dataset]
}

# 1. Khai báo trước Staging Bucket cho Dataproc
resource "google_storage_bucket" "dataproc_staging" {
  name          = "dataproc-staging-${var.project_id}"
  location      = var.region
  force_destroy = true # Cho phép xóa sạch file bên trong khi destroy
}

# 2. Khai báo trước Temp Bucket cho Dataproc
resource "google_storage_bucket" "dataproc_temp" {
  name          = "dataproc-temp-${var.project_id}"
  location      = var.region
  force_destroy = true
}