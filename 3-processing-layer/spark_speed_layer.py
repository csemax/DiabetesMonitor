import sys
import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --- KONFIGURASI REDIS ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

def send_to_redis(batch_df, batch_id):
    """
    Fungsi ini dipanggil untuk setiap micro-batch data baru.
    """
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    
    # Ambil data ke Python List
    records = batch_df.collect()
    
    if records:
        # Ambil record terakhir (terbaru)
        latest_record = records[-1].asDict()
        
        try:
            # 1. SIMPAN SNAPSHOT (Data Realtime)
            snapshot_data = {
                "heart_rate": latest_record["heart_rate"],
                "glucose_level": latest_record["glucose_level"],
                "status": latest_record["status"],
                "timestamp": latest_record["timestamp"]
            }
            r.set('sensor_data', json.dumps(snapshot_data))
            
            # 2. SIMPAN HISTORY GRAFIK
            import datetime
            readable_time = datetime.datetime.fromtimestamp(latest_record["timestamp"]).strftime('%H:%M:%S')
            
            graph_data = {
                "time": readable_time,
                "glucose": latest_record["glucose_level"]
            }
            
            # Append ke List & Trim (Jaga max 60 data)
            r.rpush('glucose_trend', json.dumps(graph_data))
            r.ltrim('glucose_trend', -60, -1)
            
            print(f"[VALID] Data Masuk: {latest_record['glucose_level']} mg/dL | HR: {latest_record['heart_rate']} | Status: {latest_record['status']}")
            
        except Exception as e:
            print(f"[ERROR REDIS] {e}")

# --- SETUP SPARK ---
spark = SparkSession.builder \
    .appName("HealthSuperApp_Validated") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema Data Sensor
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("heart_rate", IntegerType(), True),
    StructField("glucose_level", IntegerType(), True),
    StructField("temperature", DoubleType(), True)
])

# 1. BACA STREAM DARI KAFKA
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "health_stream") \
    .option("startingOffsets", "latest") \
    .load()

# 2. PARSING & VALIDASI BISNIS (BAGIAN PENTING)
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .na.drop() \
    .filter((col("heart_rate") >= 30) & (col("heart_rate") <= 250)) \
    .filter((col("glucose_level") >= 20) & (col("glucose_level") <= 1000))

# PENJELASAN VALIDASI DI ATAS:
# .na.drop() -> Buang data jika ada kolom kosong/null (misal sensor error kirim JSON rusak)
# heart_rate -> Hanya terima 30 s/d 250 BPM. (Di bawah 30 pingsan, di atas 250 mustahil)
# glucose    -> Hanya terima 20 s/d 1000 mg/dL. (Range medis yang mungkin terjadi)

# 3. LOGIKA STATUS (Hanya berjalan pada data yang Lolos Validasi)
processed_df = json_df.withColumn("status", 
    when((col("heart_rate") > 140) | (col("glucose_level") > 250), "CRITICAL_HIGH")
    .when((col("heart_rate") < 50) | (col("glucose_level") < 70), "CRITICAL_LOW")
    .when((col("heart_rate") > 110) | (col("glucose_level") > 180), "WARNING")
    .otherwise("NORMAL")
)

# 4. OUTPUT KE REDIS
query = processed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(send_to_redis) \
    .start()

query.awaitTermination()