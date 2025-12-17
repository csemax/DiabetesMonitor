import json
import csv
import os
from kafka import KafkaConsumer
from datetime import datetime

# --- KONFIGURASI ---
KAFKA_TOPIC = "health_stream"
BOOTSTRAP_SERVER = "localhost:9092"
STORAGE_DIR = "data/storage"
FILENAME = "health_data_master.csv"

# Pastikan folder penyimpanan ada
if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR)

FULL_PATH = os.path.join(STORAGE_DIR, FILENAME)

def start_archiving():
    print(f"🗄️  ARCHIVER AKTIF. Menyimpan data ke: {FULL_PATH}")
    
    # 1. Koneksi ke Kafka sebagai Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVER,
        auto_offset_reset='latest', # Baca data baru saja
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # 2. Siapkan File CSV
    # Cek apakah file sudah ada (untuk menentukan perlu tulis Header atau tidak)
    file_exists = os.path.isfile(FULL_PATH)
    
    with open(FULL_PATH, mode='a', newline='') as file:
        # Tentukan nama kolom sesuai data sensor
        fieldnames = ['timestamp', 'sensor_id', 'heart_rate', 'glucose_level', 'temperature']
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # Tulis Header kalau file baru dibuat
        if not file_exists:
            writer.writeheader()
            print("📝 File baru dibuat. Header ditulis.")

        # 3. Loop Menerima Data
        try:
            for message in consumer:
                data = message.value
                
                # Tambahkan waktu rekam (opsional, tapi bagus untuk data lake)
                # data['archived_at'] = datetime.now().isoformat()
                
                # Filter hanya kolom yang kita butuhkan agar rapi
                row = {
                    'timestamp': data.get('timestamp'),
                    'sensor_id': data.get('sensor_id'),
                    'heart_rate': data.get('heart_rate'),
                    'glucose_level': data.get('glucose_level'),
                    'temperature': data.get('temperature')
                }

                # Tulis ke CSV
                writer.writerow(row)
                
                # Pastikan data tersimpan fisik ke hardisk (Flush)
                file.flush() 
                
                print(f"[SAVED] {row['heart_rate']} bpm, {row['glucose_level']} mg/dL")

        except KeyboardInterrupt:
            print("\n🛑 Archiver dihentikan.")

if __name__ == "__main__":
    start_archiving()