import psycopg2

# Konfigurasi Koneksi
DB_CONFIG = {
    "dbname": "diabetes_db",
    "user": "admin",
    "password": "password123",
    "host": "localhost"
}

def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("--- Mereset Database ---")

    # 1. Hapus tabel lama jika ada (Urutan penting: Report dulu baru User)
    cur.execute("DROP TABLE IF EXISTS daily_reports;")
    cur.execute("DROP TABLE IF EXISTS users;")

    # 2. Buat Tabel USERS (Tabel Induk)
    # user_id di sini adalah PRIMARY KEY
    print("1. Membuat Tabel 'users'...")
    cur.execute("""
        CREATE TABLE users (
            user_id VARCHAR(50) PRIMARY KEY,
            full_name VARCHAR(100),
            age INT,
            weight_kg FLOAT,
            doctor_name VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # 3. Buat Tabel DAILY_REPORTS (Tabel Anak)
    # user_id di sini adalah FOREIGN KEY yang merujuk ke tabel users
    print("2. Membuat Tabel 'daily_reports'...")
    cur.execute("""
       CREATE TABLE daily_reports (
            report_id SERIAL PRIMARY KEY,
            report_date DATE NOT NULL,
            user_id VARCHAR(50) NOT NULL,
            
            -- Metrik Lengkap
            avg_glucose FLOAT,
            avg_heart_rate FLOAT, -- Tambahan
            avg_spo2 FLOAT,       -- Tambahan
            total_steps INT,      -- Tambahan
            
            CONSTRAINT fk_user FOREIGN KEY(user_id) REFERENCES users(user_id)
        );
    """)

    # 4. Masukkan Data Dummy User (SEEDING)
    # Kita WAJIB punya user 'USER-101' di sini, 
    # karena sensor_dummy.py mengirim data atas nama 'USER-101'.
    # Kalau user ini tidak ada, PostgreSQL akan MENOLAK data dari Spark (Error Foreign Key).
    print("3. Memasukkan Data Dummy User...")
    cur.execute("""
        INSERT INTO users (user_id, full_name, age, weight_kg, doctor_name)
        VALUES 
            ('USER-101', 'Budi Santoso', 45, 75.5, 'Dr. Tirta'),
            ('USER-102', 'Siti Aminah', 32, 55.0, 'Dr. Boyke');
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Sukses! Struktur Database Relational Siap.")

if __name__ == "__main__":
    create_tables()