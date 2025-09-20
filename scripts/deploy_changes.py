#!/usr/bin/env python3
"""scripts/deploy_changes.py

Applies idempotent schema changes with transactional safety and records applied versions in schema_versions table.

Usage:
    python scripts/deploy_changes.py --version v1_add_summary_table
"""
import os, sys, argparse
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import errorcode

load_dotenv()
DB_HOST = os.getenv('DB_HOST','localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_USER = os.getenv('DB_USER','root')
DB_PASS = os.getenv('DB_PASS','')
DB_NAME = os.getenv('DB_NAME','nyc_taxi')

def connect():
    try:
        conn = mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME, autocommit=False)
        return conn
    except mysql.connector.Error as err:
        print(f"[ERROR] DB connect failed: {err}")
        sys.exit(1)

def applied_versions(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS schema_versions (version VARCHAR(64) PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, description TEXT)")
    conn.commit()
    cur.close()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT version FROM schema_versions")
    rows = cur.fetchall()
    cur.close()
    return set(r['version'] for r in rows)

def apply_version(conn, version, ddl_sql, description=''):
    if version in applied_versions(conn):
        print(f"Version {version} already applied. Skipping.")
        return False
    cur = conn.cursor()
    try:
        cur.execute('START TRANSACTION')
        for stmt in ddl_sql.split(';'):
            s = stmt.strip()
            if not s:
                continue
            cur.execute(s)
        cur.execute('INSERT INTO schema_versions (version, description) VALUES (%s, %s)', (version, description))
        conn.commit()
        print(f"Applied version {version}")
        return True
    except Exception as e:
        print(f"[ERROR] Applying version failed: {e}. Rolling back.")
        try:
            conn.rollback()
        except:
            pass
        return False
    finally:
        cur.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', required=True, help='Version key, e.g., v1_add_summary_table')
    parser.add_argument('--description', default='', help='Short description')
    args = parser.parse_args()

    conn = connect()
    # Example DDL -- create a daily summary table
    if args.version == 'v1_add_summary_table':
        ddl = """
        CREATE TABLE IF NOT EXISTS daily_fare_summary (
            summary_date DATE,
            pulocationid INT,
            avg_fare DOUBLE,
            trip_count BIGINT,
            PRIMARY KEY (summary_date, pulocationid)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_summary_pu ON daily_fare_summary (pulocationid);
        """
    else:
        print("No predefined DDL for this version. Provide your own or edit the script.")
        conn.close()
        return

    apply_version(conn, args.version, ddl, args.description)
    conn.close()

if __name__ == '__main__':
    main()
