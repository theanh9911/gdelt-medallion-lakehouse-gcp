from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Dinh nghia cac thuoc tinh mac dinh cho DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# 2. Khoi tao DAG lap lich hang ngay (@daily)
with DAG(
    'gdel_local_medallion_pipeline',
    default_args=default_args,
    description='Pipeline tu dong hoa luong du lieu GDELT Medallion duoi local',
    schedule_interval='@daily',
    catchup=False,
    tags=['pyspark', 'medallion', 'local'],
) as dag:

    # TASK 1: Tu dong tai va giai nen du lieu tin tuc GDELT GKG moi nhat
    download_data = BashOperator(
        task_id='download_gdelt_raw_data',
        bash_command='python /opt/airflow/scripts/download_sample.py',
    )

    # TASK 2: Chay PySpark Script de thuc hien luong Medallion (Bronze -> Silver -> Gold)
    run_spark_pipeline = BashOperator(
        task_id='run_pyspark_medallion_pipeline',
        bash_command='python /opt/airflow/scripts/pyspark_eda.py',
    )

    # 3. Thiet lap quan he phu thuoc (Download xong moi duoc chay Spark)
    download_data >> run_spark_pipeline
