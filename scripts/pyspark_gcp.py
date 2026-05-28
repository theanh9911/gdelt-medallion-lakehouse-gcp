import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

def run_cloud_pipeline():
    # 1. Đọc tham số đầu vào từ Airflow
    if len(sys.argv) < 3:
        print("[CLOUD-SPARK] ❌ Thiếu đối số đầu vào! Yêu cầu: <PROJECT_ID> <DATA_LAKE_BUCKET>")
        sys.exit(1)
        
    project_id = sys.argv[1]
    data_lake_bucket = sys.argv[2]
    
    print(f"[CLOUD-SPARK] Khởi động Spark Job Medallion trên GCP Dataproc...")
    print(f"[CLOUD-SPARK] Project ID: {project_id}")
    print(f"[CLOUD-SPARK] Data Lake Bucket: {data_lake_bucket}")
    
    # 2. Khởi tạo Cloud Spark Session
    spark = SparkSession.builder \
        .appName("GDELT-Cloud-Medallion-Pipeline") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("INFO")
    
    # -------------------------------------------------------------------------
    # CHẶNG 1: TẦNG BRONZE (Lưu trữ nguyên bản dạng Parquet trên GCS)
    # -------------------------------------------------------------------------
    print("\n=== [STAGE 1: BRONZE LAYER] ===")
    gcs_raw_csv_path = f"gs://{data_lake_bucket}/bronze/*.csv"
    gcs_bronze_parquet_path = f"gs://{data_lake_bucket}/bronze_parquet"
    
    # Định nghĩa Schema 27 cột theo chuẩn GDELT GKG v2.1
    gkg_schema_full = StructType([
        StructField("RecordID", StringType(), True),
        StructField("DateRaw", StringType(), True),
        StructField("SourceCollection", StringType(), True),
        StructField("SourceName", StringType(), True),
        StructField("Url", StringType(), True),
        StructField("Counts", StringType(), True),
        StructField("V2Counts", StringType(), True),
        StructField("Themes", StringType(), True),
        StructField("V2Themes", StringType(), True),
        StructField("Locations", StringType(), True),
        StructField("V2Locations", StringType(), True),
        StructField("Persons", StringType(), True),
        StructField("V2Persons", StringType(), True),
        StructField("Organizations", StringType(), True),
        StructField("V2Organizations", StringType(), True),
        StructField("Tone", StringType(), True),
        StructField("V2EnhancedDates", StringType(), True),
        StructField("V2GCAM", StringType(), True),
        StructField("V2SharingImage", StringType(), True),
        StructField("V2RelatedImages", StringType(), True),
        StructField("V2SocialVideoEmbeds", StringType(), True),
        StructField("V2SocialImageEmbeds", StringType(), True),
        StructField("V2TranslationInfo", StringType(), True),
        StructField("V21ALLNAMES", StringType(), True),
        StructField("V21AMOUNTS", StringType(), True),
        StructField("V21DATES", StringType(), True),
        StructField("V21XMLExtras", StringType(), True)
    ])
    
    print(f"[STAGE 1] Đang đọc CSV thô từ GCS: {gcs_raw_csv_path}...")
    raw_df = spark.read \
        .option("delimiter", "\t") \
        .schema(gkg_schema_full) \
        .csv(gcs_raw_csv_path)
        
    print(f"[STAGE 1] Đang nén và lưu trữ thành Bronze Parquet tại GCS: {gcs_bronze_parquet_path}...")
    raw_df.write.mode("overwrite").parquet(gcs_bronze_parquet_path)
    print("[STAGE 1] ✅ Hoàn thành lưu trữ tầng BRONZE!")
    
    # -------------------------------------------------------------------------
    # CHẶNG 2: TẦNG SILVER (Làm sạch và tối ưu hóa kiểu dữ liệu trên GCS)
    # -------------------------------------------------------------------------
    print("\n=== [STAGE 2: SILVER LAYER] ===")
    gcs_silver_parquet_path = f"gs://{data_lake_bucket}/silver_parquet"
    
    print("[STAGE 2] Đang đọc dữ liệu từ Bronze Parquet...")
    bronze_df = spark.read.parquet(gcs_bronze_parquet_path)
    
    print("[STAGE 2] Đang làm sạch, trích xuất ToneScore và chuẩn hóa trường Date...")
    silver_df = bronze_df.withColumn("ToneScore", F.split(F.col("Tone"), ",")[0].cast("float")) \
                          .withColumn("Date", F.to_timestamp(F.col("DateRaw"), "yyyyMMddHHmmss")) \
                          .filter(F.col("ToneScore").isNotNull() & F.col("Themes").isNotNull()) \
                          .select("RecordID", "Date", "SourceName", "Url", "Themes", "ToneScore")
                          
    print(f"[STAGE 2] Đang lưu trữ Silver Parquet tại GCS: {gcs_silver_parquet_path}...")
    silver_df.write.mode("overwrite").parquet(gcs_silver_parquet_path)
    print("[STAGE 2] ✅ Hoàn thành lưu trữ tầng SILVER!")
    
    # -------------------------------------------------------------------------
    # CHẶNG 3: TẦNG GOLD (Phân rã Themes Kinh tế và ghi đè vào BigQuery Staging)
    # -------------------------------------------------------------------------
    print("\n=== [STAGE 3: GOLD LAYER] ===")
    print("[STAGE 3] Đang đọc dữ liệu từ Silver Parquet...")
    silver_clean_df = spark.read.parquet(gcs_silver_parquet_path)
    
    print("[STAGE 3] Đang phân rã (explode) cột Themes...")
    exploded_df = silver_clean_df.withColumn("ThemesArray", F.split(F.col("Themes"), ";")) \
                                  .withColumn("Theme", F.explode(F.col("ThemesArray"))) \
                                  .filter(F.col("Theme") != "")
                                  
    print("[STAGE 3] Đang lọc các chủ đề Kinh tế vĩ mô (ECON_)...")
    econ_gold_df = exploded_df.filter(F.col("Theme").startswith("ECON_"))
    
    print("[STAGE 3] Đang định dạng cột trùng khớp 100% với schema BigQuery...")
    bq_gold_output = econ_gold_df.select(
        F.col("RecordID").alias("GKGRECORDID"),
        F.col("Date").alias("DATE"),
        F.col("SourceName"),
        F.col("Url"),
        F.col("Theme"),
        F.col("ToneScore")
    )
    
    # Tên bảng Staging để nạp trung gian chống trùng lặp
    staging_table = "media_sentiment.daily_analysis_staging"
    print(f"[STAGE 3] Đang ghi đè (OVERWRITE) dữ liệu Gold vào bảng Staging: {staging_table}...")
    
    # Ghi đè vào bảng tạm BigQuery Staging
    bq_gold_output.write \
        .format("bigquery") \
        .option("parentProject", project_id) \
        .option("project", project_id) \
        .option("dataset", "media_sentiment") \
        .option("table", staging_table) \
        .option("temporaryGcsBucket", data_lake_bucket) \
        .mode("overwrite") \
        .save()
        
    print("[STAGE 3] ✅ Hoàn thành xuất bản Gold Staging lên BigQuery!")
    
    spark.stop()
    print("[CLOUD-SPARK] 🎉 Toàn bộ quy trình Medallion 3 Layer kết thúc thành công!")

if __name__ == "__main__":
    run_cloud_pipeline()
