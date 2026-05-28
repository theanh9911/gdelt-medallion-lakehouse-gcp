import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F

def run_medallion_pipeline():
    # 1. Tu dong cau hinh HADOOP_HOME de chạy muot ma tren Windows ma khong bi loi winutils
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
    
    # 2. Khoi tao local Spark Session
    spark = SparkSession.builder \
        .appName("GDELT-Medallion-Pipeline") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    print("[SPARK] Khoi tao PySpark Session thanh cong!")
    
    # 3. Tim file CSV da tai ve trong thu muc data/
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_files = glob.glob(os.path.join(base_dir, "data", "*.gkg.csv"))
    if not data_files:
        print("[SPARK] ❌ Khong tim thay file .gkg.csv nao trong thu muc data/.")
        return
        
    gkg_file_path = data_files[0]
    print(f"[SPARK] Da phat hien file du lieu thao: {gkg_file_path}")
    
    # 4. Dinh nghia Schema day du 27 cot theo Codebook v2.1 chinh thuc
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
    
    # ==========================================
    # CHẶNG 1: TẦNG BRONZE (Lưu tru nguyen ban)
    # ==========================================
    print("\n--- [STAGE 1: BRONZE LAYER] ---")
    print("[SPARK] Dang nap du lieu tho...")
    raw_df = spark.read \
        .option("delimiter", "\t") \
        .schema(gkg_schema_full) \
        .csv(gkg_file_path)
        
    total_rows = raw_df.count()
    print(f"[SPARK] Đã load thanh cong: {total_rows:,} dong ban ghi.")
    
    bronze_dir = os.path.join(base_dir, "data", "bronze")
    print(f"[SPARK] Dang ghi du lieu tho ra file Parquet tai: {bronze_dir}...")
    raw_df.write.mode("overwrite").parquet(bronze_dir)
    print("[SPARK] ✅ Hoan thanh ghi du lieu vao tang BRONZE!")
    
    # ==========================================
    # CHẶNG 2: TẦNG SILVER (Lam sach du lieu)
    # ==========================================
    print("\n--- [STAGE 2: SILVER LAYER] ---")
    print("[SPARK] Dang doc du lieu tu Bronze Lake...")
    bronze_df = spark.read.parquet(bronze_dir)
    
    print("[SPARK] Dang tien hanh lam sach va ep kieu du lieu...")
    # - Trich xuat ToneScore tu cot Tone (cot so 15)
    # - Ep kieu thoi gian sang Timestamp chuẩn
    # - Loc bo cac dong bi NULL
    silver_df = bronze_df.withColumn("ToneScore", F.split(F.col("Tone"), ",")[0].cast("float")) \
                          .withColumn("Date", F.to_timestamp(F.col("DateRaw"), "yyyyMMddHHmmss")) \
                          .filter(F.col("ToneScore").isNotNull() & F.col("Themes").isNotNull()) \
                          .select("RecordID", "Date", "SourceName", "Url", "Themes", "Organizations", "ToneScore", "V2SharingImage")
                          
    silver_dir = os.path.join(base_dir, "data", "silver")
    print(f"[SPARK] Dang ghi du lieu sach ra file Parquet tai: {silver_dir}...")
    silver_df.write.mode("overwrite").parquet(silver_dir)
    print("[SPARK] ✅ Hoan thanh ghi du lieu vao tang SILVER!")
    
    # ==========================================
    # CHẶNG 3: TẦNG GOLD (Phan tich xu huong)
    # ==========================================
    print("\n--- [STAGE 3: GOLD LAYER] ---")
    print("[SPARK] Dang doc du lieu sach tu Silver Lake...")
    silver_clean_df = spark.read.parquet(silver_dir)
    
    print("[SPARK] Dang bock tach va explode cac chu de Themes...")
    # - Split va explode Themes
    # - Loc chu de ECON_
    gold_exploded = silver_clean_df.withColumn("ThemesArray", F.split(F.col("Themes"), ";")) \
                                   .withColumn("Theme", F.explode(F.col("ThemesArray"))) \
                                   .filter(F.col("Theme") != "")
                                   
    gold_econ_trends = gold_exploded.filter(F.col("Theme").startswith("ECON_")) \
                                    .groupBy("Theme") \
                                    .agg(
                                        F.count("RecordID").alias("ArticleCount"),
                                        F.round(F.avg("ToneScore"), 2).alias("AvgTone")
                                     ) \
                                    .filter(F.col("ArticleCount") >= 5) \
                                    .orderBy(F.desc("ArticleCount"))
                                    
    gold_dir = os.path.join(base_dir, "data", "gold")
    print(f"[SPARK] Dang ghi ket qua curated ra file Parquet tai: {gold_dir}...")
    gold_econ_trends.write.mode("overwrite").parquet(gold_dir)
    print("[SPARK] ✅ Hoan thanh ghi du lieu vao tang GOLD!")
    
    # Hiển thị kết quả kiểm chứng
    print("\n--- KET QUA PHAN TICH XU HUONG KINH TE (TANG GOLD) ---")
    gold_econ_trends.show(10, truncate=False)
    
    print("\n[SPARK] 🎉 Toan bo Medallion Pipeline local da chạy hoan toan thanh cong!")
    spark.stop()

if __name__ == "__main__":
    run_medallion_pipeline()
