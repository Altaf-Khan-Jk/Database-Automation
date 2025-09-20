#!/usr/bin/env python3
"""scripts/data_ingestion.py

Downloads a TLC month CSV (default example URL commented), reads in chunks, cleans, and batch-inserts into MySQL.
Logs rows/sec, memory usage, and verifies row counts and a sample aggregate (average fare per hour).

Usage example:
    python scripts/data_ingestion.py --url "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2021-01.csv" --chunksize 15000
"""
import os, sys, argparse, time, csv, tempfile, math
from urllib.request import urlopen, Request
from dotenv import load_dotenv
import pandas as pd
import psutil
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
from sqlalchemy import create_engine, text

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', '')
DB_NAME = os.getenv('DB_NAME', 'nyc_taxi')

DEFAULT_URL = "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2021-01.csv"

def connect_db():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME, autocommit=False
        )
        return conn
    except mysql.connector.Error as err:
        print(f"[ERROR] DB connection failed: {err}")
        sys.exit(1)

def stream_download(url, tmp_path):
    print(f"Downloading from: {url}")
    req = Request(url, headers={'User-Agent': 'assignment-client/1.0'})
    with urlopen(req) as r, open(tmp_path, 'wb') as f:
        while True:
            chunk = r.read(1024*8)
            if not chunk:
                break
            f.write(chunk)
    print("Download complete.")

def clean_chunk(df):
    # Basic cleaning: parse datetimes, drop invalid rows, negative fares/trip distance
    for col in ['pickup_datetime','dropoff_datetime']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    # numeric conversions
    num_cols = ['passenger_count','trip_distance','fare_amount','tip_amount','total_amount']
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    # drop rows with missing pickup/dropoff datetimes
    df = df.dropna(subset=['pickup_datetime','dropoff_datetime'], how='any')
    # remove negative fare or trip distance or unrealistic passenger_count
    df = df[(df['fare_amount'].fillna(0) >= 0) & (df['trip_distance'].fillna(0) >= 0) & (df['passenger_count'].fillna(0) >= 0)]
    return df

def chunk_and_insert(file_path, table='trips_raw', chunksize=10000, batch_size=5000):
    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_pre_ping=True)
    total_rows = 0
    start_time = time.time()
    proc = psutil.Process()
    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize, low_memory=False)):
        t0 = time.time()
        original_len = len(chunk)
        chunk = clean_chunk(chunk)
        cleaned_len = len(chunk)
        if cleaned_len == 0:
            print(f"Chunk {i}: cleaned out all rows ({original_len} -> 0). Skipping.")
            continue
        # rename columns if needed to match schema (common TLC names vary)
        col_map = {}
        if 'lpep_pickup_datetime' in chunk.columns:
            col_map['lpep_pickup_datetime'] = 'pickup_datetime'
        if 'lpep_dropoff_datetime' in chunk.columns:
            col_map['lpep_dropoff_datetime'] = 'dropoff_datetime'
        if 'PULocationID' in chunk.columns:
            col_map['PULocationID'] = 'PULocationID'
        chunk = chunk.rename(columns=col_map)
        # select only columns we have in schema
        allowed = ['vendorid','vendor_id','pickup_datetime','dropoff_datetime','passenger_count','trip_distance','rate_code','store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','total_amount']
        present = [c for c in chunk.columns if c in allowed]
        df = chunk[present].copy()
        # normalize column names to schema's names
        df = df.rename(columns={ 'vendorid':'vendor_id', 'vendor':'vendor_id', 'VendorID':'vendor_id' })
        # ensure numeric columns are native python types for executemany
        df = df.replace({pd.NaT: None})
        records = df.to_dict(orient='records')
        # insert in batches
        inserted = 0
        conn = engine.raw_connection()
        cursor = conn.cursor()
        keys = df.columns.tolist()
        placeholders = ','.join(['%s'] * len(keys))
        cols_sql = ','.join([f'`{k}`' for k in keys])
        sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
        try:
            for j in range(0, len(records), batch_size):
                batch = records[j:j+batch_size]
                values = []
                for r in batch:
                    values.append(tuple(r.get(k, None) for k in keys))
                cursor.executemany(sql, values)
                conn.commit()
                inserted += len(batch)
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"[ERROR] Insert failed on chunk {i}: {e}")
            try:
                conn.rollback()
            except:
                pass
        dt = time.time() - t0
        total_rows += inserted
        mem = proc.memory_info().rss / (1024*1024)
        rps = inserted / dt if dt > 0 else float('inf')
        print(f"Chunk {i}: read={original_len} cleaned={cleaned_len} inserted={inserted} time={dt:.2f}s rows/sec={rps:.1f} mem={mem:.1f}MB")
    total_time = time.time() - start_time
    print(f"Total inserted: {total_rows} in {total_time:.2f}s ({total_rows/total_time if total_time>0 else 0:.1f} rows/sec)")
    return total_rows

def verify_counts_and_aggregates(table='trips_raw'):
    conn = connect_db()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
        cnt = cur.fetchone()['cnt']
        cur.execute(f"SELECT HOUR(pickup_datetime) as hr, AVG(fare_amount) as avg_fare FROM {table} GROUP BY hr ORDER BY hr LIMIT 5")
        sample = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return cnt, sample

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default=DEFAULT_URL, help='CSV URL to download (TLC month)')
    parser.add_argument('--chunksize', type=int, default=20000, help='pandas chunksize')
    parser.add_argument('--batch-size', type=int, default=5000, help='insert batch size')
    parser.add_argument('--no-download', action='store_true', help='if file is already downloaded skip download')
    parser.add_argument('--file', help='path to local CSV file to use instead of downloading')
    args = parser.parse_args()

    tmpfile = None
    try:
        if args.file:
            csv_path = args.file
        else:
            tmpdir = tempfile.gettempdir()
            tmpfile = os.path.join(tmpdir, f"tlc_month_{int(time.time())}.csv")
            if not args.no_download:
                stream_download(args.url, tmpfile)
            csv_path = tmpfile
        inserted = chunk_and_insert(csv_path, chunksize=args.chunksize, batch_size=args.batch_size)
        cnt, sample = verify_counts_and_aggregates()
        print(f"Verify: total rows in table = {cnt}")
        print("Sample aggregates (hour, avg_fare):", sample)
    finally:
        if tmpfile and os.path.exists(tmpfile):
            try:
                os.remove(tmpfile)
            except Exception:
                pass

if __name__ == '__main__':
    main()
