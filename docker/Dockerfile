# Sử dụng ảnh Airflow chính thức làm nền tảng
FROM apache/airflow:2.7.0-python3.11

USER root

# Cài đặt OpenJDK-17 (Java là bắt buộc để chạy PySpark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường JAVA_HOME chuẩn Docker
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Cài đặt thêm các thư viện python cần thiết trực tiếp vào ảnh
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    pandas \
    requests
