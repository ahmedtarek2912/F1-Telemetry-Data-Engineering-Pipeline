import ast
from datetime import datetime
import glob
import os
import shutil
import psycopg2
from psycopg2.extras import execute_values

DB = {
    'host': 'localhost',
    'port': '5432',
    'database': 'f1',
    'user': 'postgres',
    'password': 'atarek',
}


def parse_date(d_str):
    for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d']:
        try:
            return datetime.strptime(d_str, fmt)
        except ValueError:
            continue
    return None


def load_cleansed_to_postgres():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    # Reset tables to prevent duplicate records on repeated runs
    cur.execute(
        "TRUNCATE TABLE car_telemetry, error_logs RESTART IDENTITY;"
    )

    c_arch = os.path.join('cleaned', 'archive')
    e_arch = os.path.join('error_logs', 'archive')

    os.makedirs(c_arch, exist_ok=True)
    os.makedirs(e_arch, exist_ok=True)

    total_clean, total_errors = 0, 0

    # =========================================================
    # 1. Bulk Load Clean Telemetry Records
    # =========================================================
    clean_files = [
        f
        for f in glob.glob('cleaned/clean_telemetry*')
        if 'archive' not in f
    ]

    clean_insert_query = """
        INSERT INTO car_telemetry (
            event_date,
            rpm,
            speed,
            gear,
            throttle,
            brake,
            drs,
            source,
            event_time,
            session_time,
            driver_number
        )
        VALUES %s
    """

    for file_path in clean_files:
        clean_batch = []

        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    rec = ast.literal_eval(line.strip())
                    dt = parse_date(rec['Date'])

                    if not dt:
                        continue

                    clean_batch.append((
                        dt,
                        rec['RPM'],
                        rec['Speed'],
                        rec['nGear'],
                        rec['Throttle'],
                        rec['Brake'],
                        rec['DRS'],
                        rec['Source'],
                        rec['Time'],
                        rec['SessionTime'],
                        rec['Driver'],
                    ))

                    # Insert in chunks of 10,000 rows
                    if len(clean_batch) >= 10000:
                        execute_values(
                            cur,
                            clean_insert_query,
                            clean_batch
                        )
                        total_clean += len(clean_batch)
                        clean_batch = []

                except Exception:
                    continue

        # Insert remaining rows for this file
        if clean_batch:
            execute_values(
                cur,
                clean_insert_query,
                clean_batch
            )
            total_clean += len(clean_batch)

        # Archive processed file
        shutil.move(
            file_path,
            os.path.join(c_arch, os.path.basename(file_path))
        )

    # =========================================================
    # 2. Bulk Load Error Logs
    # =========================================================
    error_files = [
        f
        for f in glob.glob('error_logs/corrupted_telemetry*')
        if 'archive' not in f
    ]

    error_insert_query = """
        INSERT INTO error_logs (
            source_file,
            error_reason,
            raw_record
        )
        VALUES %s
    """

    for file_path in error_files:
        error_batch = []

        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    raw_text = line.strip()
                    reason = 'Validation Failed'

                    if 'Error:' in raw_text and '|' in raw_text:
                        parts = raw_text.split('|', 1)
                        reason = (
                            parts[0]
                            .replace('Error:', '')
                            .strip()
                        )
                        raw_text = (
                            parts[1]
                            .replace('Raw Line:', '')
                            .strip()
                        )

                    error_batch.append((
                        'f1_car_data_dirty.csv',
                        reason,
                        raw_text
                    ))

                    # Insert in chunks of 10,000 rows
                    if len(error_batch) >= 10000:
                        execute_values(
                            cur,
                            error_insert_query,
                            error_batch
                        )
                        total_errors += len(error_batch)
                        error_batch = []

                except Exception:
                    continue

        # Insert remaining error rows for this file
        if error_batch:
            execute_values(
                cur,
                error_insert_query,
                error_batch
            )
            total_errors += len(error_batch)

        # Archive processed file
        shutil.move(
            file_path,
            os.path.join(e_arch, os.path.basename(file_path))
        )

    # =========================================================
    # 3. Commit & Clean Up
    # =========================================================
    conn.commit()
    cur.close()
    conn.close()

    print('[SUCCESS] Database load completed successfully!')
    print(f'   - Clean records added: {total_clean}')
    print(f'   - Errors added: {total_errors}')


if __name__ == '__main__':
    load_cleansed_to_postgres()