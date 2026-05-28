variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "Region for Google Cloud resources"
  type        = string
  default     = "us-central1"
}

variable "data_lake_bucket_name" {
  description = "Tên duy nhất toàn cầu cho GCS Data Lake"
  type        = string
}

variable "code_scripts_bucket_name" {
  description = "Tên duy nhất toàn cầu cho GCS Code Scripts"
  type        = string
}
