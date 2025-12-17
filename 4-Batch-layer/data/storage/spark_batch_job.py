from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, count, min
import redis
import json
import os

# --- KONFIGURASI ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# Lokasi File CSV
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "data", "storage", "*.csv")

def run_batch_analytics():
    print("🚀 Memulai Spark Batch Job...")

    # 1. Inisialisasi Spark Session
    spark = SparkSession.builder \
        .appName("HealthBatchAnalytics") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR") # Supaya log tidak berisik

    try:
        # 2. Baca Data dari Data Lake (CSV)
        # inferSchema=True agar angka terbaca sebagai angka (bukan string)
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(CSV_PATH)
        
        # Cek jika data kosong
        if df.count() == 0:
            print("⚠️ Data Lake masih kosong. Nyalakan Archiver & Sensor dulu.")
            return

        print(f"📊 Memproses {df.count()} baris data historis...")

        # 3. Hitung Agregasi (Rata-rata, Max, Total)
        # Kita hitung statistik global (seluruh data)
        stats_df = df.agg(
            avg("glucose_level").alias("avg_glucose"),
            avg("heart_rate").alias("avg_heart"),
            max("glucose_level").alias("max_glucose"),
            max("heart_rate").alias("max_heart"),
            count("sensor_id").alias("total_records")
        )
        
        # Ambil hasilnya ke Python Object
        result = stats_df.collect()[0]
        
        # Format Data untuk Redis
        batch_data = {
            "avg_glucose": round(result["avg_glucose"], 1),
            "avg_heart": round(result["avg_heart"], 1),
            "max_glucose": result["max_glucose"],
            "max_heart": result["max_heart"],
            "total_records": result["total_records"]
        }

        # 4. Simpan ke Redis (Agar Dashboard bisa baca)
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        r.set("batch_stats", json.dumps(batch_data))
        
        print("\n✅ ANALISA SELESAI!")
        print("========================")
        print(json.dumps(batch_data, indent=4))
        print("========================")
        print("Data terkirim ke Redis key: 'batch_stats'")

    except Exception as e:
        print(f"❌ Error: {e}")
        # Biasanya error kalau file CSV belum ada sama sekali

    finally:
        spark.stop()

if __name__ == "__main__":
    run_batch_analytics()