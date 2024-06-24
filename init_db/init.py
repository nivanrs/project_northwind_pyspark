import pandas as pd
from sqlalchemy import create_engine, Date
import os

def is_date_column(series, threshold=0.8):
    """
    Cek apakah suatu kolom seharusnya bertipe DATE dengan mencoba mengonversinya ke datetime.
    Jika persentase nilai yang berhasil dikonversi lebih dari threshold, return True.
    """
    converted = pd.to_datetime(series, errors='coerce')
    non_na_ratio = converted.notna().mean()
    return non_na_ratio > threshold

def upload_csv_to_postgres(file_paths, db_url):
    # Membuat koneksi ke database PostgreSQL
    engine = create_engine(db_url)

    for file_path in file_paths:
        # Memuat file CSV ke DataFrame
        df = pd.read_csv(file_path)
        
        # Menyimpan tipe data asli
        original_dtypes = df.dtypes

        # Mendeteksi kolom yang seharusnya bertipe DATE, kecuali yang bertipe integer atau float
        date_columns = [
            col for col in df.columns
            if col not in df.select_dtypes(include=['int64', 'float64']).columns and is_date_column(df[col])
        ]
        
        # Mengonversi kolom yang terdeteksi menjadi tipe datetime
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Mengambil nama tabel dari nama file CSV
        table_name = os.path.splitext(os.path.basename(file_path))[0]

        # Membuat dictionary untuk dtype hanya untuk kolom tanggal
        dtype = {col: Date() for col in date_columns}

        # Mengunggah DataFrame ke PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype)

        print(f"Data dari file '{file_path}' berhasil diunggah ke tabel '{table_name}' di database.")

# Contoh penggunaan
csv_folder_path = './csv'  # Ganti dengan path folder yang berisi file CSV Anda
db_url = 'postgresql://postgres:12345678@localhost:5431/northwind-pyspark'  # Ganti dengan URL koneksi PostgreSQL Anda

# Mendapatkan daftar file CSV dalam folder
csv_files = [os.path.join(csv_folder_path, f) for f in os.listdir(csv_folder_path) if f.endswith('.csv')]

# Mengunggah semua file CSV ke PostgreSQL
upload_csv_to_postgres(csv_files, db_url)
