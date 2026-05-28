from datetime import datetime, timedelta
import os
import requests
import zipfile
from io import BytesIO

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)

# =========================================================================
# ĐỌC BIẾN TẬP TRUNG TỪ AIRFLOW VARIABLES (BỎ HOÀN TOÀN CẤU HÌNH CỨNG)
# =========================================================================
PROJECT_ID        = Variable.get("gcp_project_id", default_var="media-sentiment-pipeline")
CODE_BUCKET       = Variable.get("gcs_script_bucket", default_var="gdel-code-scripts-theanh-9911")
DATA_LAKE_BUCKET  = Variable.get("gcs_data_lake_bucket", default_var="gdel-data-lake-theanh-9911")

REGION       = "us-central1"
CLUSTER_NAME = "gdel-spark-cluster-theanh"

# Cấu hình cụm máy chủ ảo Dataproc (1 Master, 2 Workers)
# Chỉ định rõ các GCS Buckets trung gian do Terraform quản lý
CLUSTER_CONFIG = {
    "config_bucket": f"dataproc-staging-{PROJECT_ID}",
    "temp_bucket": f"dataproc-temp-{PROJECT_ID}",
    "gce_cluster_config": {
        "service_account": "957030226747-compute@developer.gserviceaccount.com",
        "service_account_scopes": [
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 50}
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 50}
    }
}

# Cấu hình PySpark Job gửi lên cụm Dataproc
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{CODE_BUCKET}/pyspark_gcp.py",
        "args": [PROJECT_ID, DATA_LAKE_BUCKET]
    }
}

# Câu lệnh SQL MERGE (Upsert) - Chốt chặn chống trùng lặp dữ liệu tuyệt đối
MERGE_SQL = f"""
MERGE `{PROJECT_ID}.media_sentiment.daily_analysis` T
USING `{PROJECT_ID}.media_sentiment.daily_analysis_staging` S
ON T.GKGRECORDID = S.GKGRECORDID AND T.Theme = S.Theme
WHEN MATCHED THEN
  UPDATE SET 
    T.DATE = S.DATE,
    T.SourceName = S.SourceName,
    T.Url = S.Url,
    T.ToneScore = S.ToneScore
WHEN NOT MATCHED THEN
  INSERT (GKGRECORDID, DATE, SourceName, Url, Theme, ToneScore)
  VALUES (S.GKGRECORDID, S.DATE, S.SourceName, S.Url, S.Theme, S.ToneScore);
"""

# =========================================================================
# CÁC HÀM XỬ LÝ PYTHON (INGESTION & DEPLOYMENT)
# =========================================================================

def upload_spark_script_to_gcs():
    """Tự động đồng bộ file Spark local lên GCS trước khi cụm chạy"""
    local_script_path = "/opt/airflow/scripts/pyspark_gcp.py"
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    
    print(f"[AIRFLOW] Đang đồng bộ {local_script_path} lên gs://{CODE_BUCKET}/pyspark_gcp.py...")
    gcs_hook.upload(
        bucket_name=CODE_BUCKET,
        object_name="pyspark_gcp.py",
        filename=local_script_path
    )
    print("[AIRFLOW] ✅ Đồng bộ script PySpark thành công!")

def download_and_upload_gdelt_to_gcs():
    """Tải N file dữ liệu thô gần nhất từ GDELT, giải nén và tải trực tiếp lên GCS Bronze Lake để Spark xử lý song song"""
    NUM_FILES = int(Variable.get("gdelt_num_files_ingest", default_var="10"))
    
    master_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    print(f"[AIRFLOW] Đang quét danh sách để tải {NUM_FILES} file GDELT mới nhất...")
    response = requests.get(master_url, timeout=15)
    response.raise_for_status()
    
    lines = response.text.splitlines()
    gkg_urls = []
    
    for line in reversed(lines):
        parts = line.split()
        if len(parts) == 3:
            url = parts[2]
            if ".gkg.csv.zip" in url:
                gkg_urls.append(url)
                if len(gkg_urls) >= NUM_FILES:
                    break
                    
    if not gkg_urls:
        raise Exception("Không tìm thấy file dữ liệu GKG v2 nào!")
        
    print(f"[AIRFLOW] Đã tìm thấy {len(gkg_urls)} file phù hợp để tải.")
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    
    try:
        existing_files = gcs_hook.list(bucket_name=DATA_LAKE_BUCKET, prefix="bronze/")
        if existing_files:
            print(f"[AIRFLOW] Dọn dẹp {len(existing_files)} file cũ trong gs://{DATA_LAKE_BUCKET}/bronze/...")
            for f_to_del in existing_files:
                gcs_hook.delete(bucket_name=DATA_LAKE_BUCKET, object_name=f_to_del)
    except Exception as e:
        print(f"[AIRFLOW] Cảnh báo dọn dẹp Bronze (không ảnh hưởng): {e}")

    temp_dir = "/tmp/gdelt_airflow"
    os.makedirs(temp_dir, exist_ok=True)
    
    for idx, gkg_url in enumerate(gkg_urls, 1):
        filename = os.path.basename(gkg_url)
        csv_filename = filename.replace(".zip", "")
        temp_zip_path = os.path.join(temp_dir, filename)
        extracted_csv_path = os.path.join(temp_dir, csv_filename)
        
        print(f"\n[AIRFLOW] [{idx}/{NUM_FILES}] Đang tải: {filename}...")
        try:
            file_response = requests.get(gkg_url, stream=True)
            file_response.raise_for_status()
            
            with open(temp_zip_path, "wb") as f:
                f.write(file_response.content)
                
            print(f"[AIRFLOW] Giải nén file ZIP...")
            with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                
            print(f"[AIRFLOW] Đẩy lên GCS: gs://{DATA_LAKE_BUCKET}/bronze/{csv_filename}")
            gcs_hook.upload(
                bucket_name=DATA_LAKE_BUCKET,
                object_name=f"bronze/{csv_filename}",
                filename=extracted_csv_path
            )
            
        except Exception as file_error:
            print(f"[AIRFLOW] ❌ Lỗi xử lý file {filename}: {file_error}")
            continue
        finally:
            if os.path.exists(temp_zip_path):
                os.remove(temp_zip_path)
            if os.path.exists(extracted_csv_path):
                os.remove(extracted_csv_path)
                
    print("\n[AIRFLOW] ✅ Hoàn thành nạp toàn bộ dữ liệu thô lên GCS Bronze Lake!")

# =========================================================================
# KHỞI TẠO DAG
# =========================================================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gdel_gcp_dataproc_pipeline',
    default_args=default_args,
    description='Pipeline tu dong E2E dong bo code, nap raw len GCS, khoi tao Dataproc va ghi BigQuery',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1, # Chặn hoàn toàn việc chạy song song chồng chéo
    tags=['gcp', 'dataproc', 'spark', 'production', 'e2e', 'dedup'],
) as dag:

    # TASK 1: Đồng bộ file PySpark Cloud lên GCS
    sync_script = PythonOperator(
        task_id="sync_spark_script_to_gcs",
        python_callable=upload_spark_script_to_gcs
    )

    # TASK 2: Tải dữ liệu thô GDELT và đẩy lên Bronze Lake
    ingest_raw = PythonOperator(
        task_id="ingest_raw_gdelt_data",
        python_callable=download_and_upload_gdelt_to_gcs
    )

    # TASK 3: Tự động khởi tạo cụm Spark ảo Dataproc (1 Master, 2 Workers)
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id="google_cloud_default"
    )

    # TASK 4: Gửi PySpark Job thực thi tính toán phân tán ghi BigQuery
    submit_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default"
    )

    # TASK 5: Thực thi SQL MERGE hợp nhất dữ liệu từ Staging vào Production (Chống trùng lặp tuyệt đối)
    merge_gold_to_production = BigQueryExecuteQueryOperator(
        task_id="merge_gold_to_production",
        sql=MERGE_SQL,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default"
    )

    # TASK 6: Tự động XÓA cụm Dataproc ngay khi chạy xong để an toàn chi phí
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",
        gcp_conn_id="google_cloud_default"
    )

    # Thiết lập luồng chạy tuần tự chặt chẽ chống trùng lặp dữ liệu:
    [sync_script, ingest_raw] >> create_cluster >> submit_job >> merge_gold_to_production >> delete_cluster
