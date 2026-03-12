#!/usr/bin/env python3
"""
Northwind Analytics — Data Loader
==================================
Loads generated CSV data into PostgreSQL (designed for Neon.tech free tier).

Usage:
    python load_data.py --mode init     # Run schema + bulk load all CSVs
    python load_data.py --mode daily    # Load only today's incremental data

Requires DATABASE_URL environment variable pointing to a PostgreSQL instance.
"""

import argparse
import csv
import io
import os
import sys

import psycopg2

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
SCHEMA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

# Tables in dependency order (respecting foreign keys)
LOAD_ORDER = [
    "employees",
    "plans",
    "companies",
    "subscriptions",
    "invoices",
    "product_usage",
    "support_tickets",
    "deals",
    "events",
    "nps_surveys",
]

# Column lists for each table (must match CSV headers and schema)
TABLE_COLUMNS = {
    "employees": [
        "employee_id", "full_name", "role", "department", "hire_date", "region", "is_active",
    ],
    "plans": [
        "plan_id", "plan_name", "tier", "monthly_price", "annual_price", "max_users", "features",
    ],
    "companies": [
        "company_id", "company_name", "industry", "employee_count", "region", "country",
        "created_at", "status", "assigned_csm", "assigned_rep",
    ],
    "subscriptions": [
        "subscription_id", "company_id", "plan_id", "start_date", "end_date",
        "mrr", "arr", "billing_cycle", "status",
    ],
    "invoices": [
        "invoice_id", "subscription_id", "company_id", "amount", "currency",
        "issued_date", "due_date", "paid_date", "status",
    ],
    "product_usage": [
        "usage_id", "company_id", "usage_date", "daily_active_users", "queries_run",
        "dashboards_viewed", "reports_exported", "api_calls", "sessions",
    ],
    "support_tickets": [
        "ticket_id", "company_id", "created_at", "resolved_at", "category",
        "priority", "status", "csat_score", "assigned_agent",
    ],
    "deals": [
        "deal_id", "company_name", "deal_name", "stage", "amount", "close_date",
        "probability", "owner", "source", "created_at", "days_in_stage", "lost_at_stage",
    ],
    "events": [
        "event_id", "company_id", "event_type", "event_timestamp", "user_id", "properties",
    ],
    "nps_surveys": [
        "survey_id", "company_id", "score", "response_date", "feedback_text", "category",
    ],
}

# Daily-mode CSV filenames map to their target tables
DAILY_FILES = {
    "product_usage_daily.csv": "product_usage",
    "support_tickets_daily.csv": "support_tickets",
    "deals_daily.csv": "deals",
    "events_daily.csv": "events",
    "nps_surveys_daily.csv": "nps_surveys",
}


def get_connection():
    """Create a connection using DATABASE_URL."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL environment variable is not set.")
        print("Set it to your Neon PostgreSQL connection string, e.g.:")
        print("  export DATABASE_URL='postgresql://user:pass@host/dbname?sslmode=require'")
        sys.exit(1)

    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"ERROR: Could not connect to database: {e}")
        sys.exit(1)


def run_schema(conn):
    """Execute the schema.sql file to create/recreate all tables."""
    print("Running schema.sql...")
    with open(SCHEMA_FILE, "r") as f:
        sql = f.read()

    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
        print("  Schema created successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"ERROR running schema: {e}")
        sys.exit(1)
    finally:
        cur.close()


def bulk_load_csv(conn, table_name: str, csv_filename: str):
    """
    Load a CSV file into a table using COPY for maximum speed.
    Falls back to INSERT if COPY is not available.
    """
    filepath = os.path.join(DATA_DIR, csv_filename)
    if not os.path.exists(filepath):
        print(f"  SKIP {csv_filename} (file not found)")
        return 0

    columns = TABLE_COLUMNS[table_name]
    cur = conn.cursor()
    row_count = 0

    try:
        # Read CSV and use copy_expert for fast bulk loading
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Build a StringIO buffer for COPY
            buf = io.StringIO()
            for row in reader:
                values = []
                for col in columns:
                    val = row.get(col, "")
                    if val == "" or val is None or val == "None":
                        values.append("\\N")  # NULL marker for COPY
                    else:
                        # Escape tabs and newlines for COPY format
                        val = str(val).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
                        values.append(val)
                buf.write("\t".join(values) + "\n")
                row_count += 1

            buf.seek(0)
            col_list = ", ".join(columns)
            copy_sql = f"COPY {table_name} ({col_list}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
            cur.copy_expert(copy_sql, buf)

        conn.commit()
        print(f"  {table_name}: {row_count:,} rows loaded")
        return row_count

    except psycopg2.Error as e:
        conn.rollback()
        print(f"  ERROR loading {table_name}: {e}")
        return 0
    finally:
        cur.close()


def reset_sequences(conn):
    """Reset serial sequences to the max ID in each table after bulk load."""
    print("Resetting sequences...")
    cur = conn.cursor()
    sequence_map = {
        "employees": ("employee_id", "employees_employee_id_seq"),
        "plans": ("plan_id", "plans_plan_id_seq"),
        "companies": ("company_id", "companies_company_id_seq"),
        "subscriptions": ("subscription_id", "subscriptions_subscription_id_seq"),
        "invoices": ("invoice_id", "invoices_invoice_id_seq"),
        "product_usage": ("usage_id", "product_usage_usage_id_seq"),
        "support_tickets": ("ticket_id", "support_tickets_ticket_id_seq"),
        "deals": ("deal_id", "deals_deal_id_seq"),
        "events": ("event_id", "events_event_id_seq"),
        "nps_surveys": ("survey_id", "nps_surveys_survey_id_seq"),
    }

    try:
        for table, (id_col, seq_name) in sequence_map.items():
            cur.execute(f"SELECT COALESCE(MAX({id_col}), 0) + 1 FROM {table}")
            next_val = cur.fetchone()[0]
            cur.execute(f"SELECT setval('{seq_name}', {next_val}, false)")
        conn.commit()
        print("  Sequences reset successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"  WARNING: Could not reset sequences: {e}")
    finally:
        cur.close()


def init_mode(conn):
    """Full initialization: schema + bulk load all CSVs."""
    run_schema(conn)

    print()
    print("Bulk loading CSV data...")
    total_rows = 0
    for table in LOAD_ORDER:
        csv_file = f"{table}.csv"
        total_rows += bulk_load_csv(conn, table, csv_file)

    reset_sequences(conn)

    print()
    print(f"Init complete. {total_rows:,} total rows loaded.")


def daily_mode(conn):
    """Load only today's incremental CSV files."""
    print("Loading daily incremental data...")
    total_rows = 0

    for csv_file, table in DAILY_FILES.items():
        filepath = os.path.join(DATA_DIR, csv_file)
        if os.path.exists(filepath):
            total_rows += bulk_load_csv(conn, table, csv_file)

    print()
    print(f"Daily load complete. {total_rows:,} rows loaded.")


def main():
    parser = argparse.ArgumentParser(description="Northwind Analytics Data Loader")
    parser.add_argument(
        "--mode",
        choices=["init", "daily"],
        default="init",
        help="'init' runs schema + full load; 'daily' loads incremental files only",
    )
    args = parser.parse_args()

    conn = get_connection()
    try:
        if args.mode == "init":
            init_mode(conn)
        else:
            daily_mode(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
