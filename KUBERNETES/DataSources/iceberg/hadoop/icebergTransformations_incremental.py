"""
icebergTransformations_incremental.py
======================================
Incremental, re-runnable version of the Iceberg evolution script.

Each invocation applies only the *next* pending transformation and records the
completed step in `evolution_state.json` (same directory as this script).
Run it once, re-ingest into DataHub, then run it again to see the next change.

Usage
-----
  python icebergTransformations_incremental.py            # run next pending step
  python icebergTransformations_incremental.py --all      # run all remaining steps
  python icebergTransformations_incremental.py --step 3   # run step 3 (force, ignores state)
  python icebergTransformations_incremental.py --steps 2,4,6
  python icebergTransformations_incremental.py --list     # show status and exit
  python icebergTransformations_incremental.py --reset    # clear state file and exit

Steps
-----
  1  · Incremental data append          → new snapshot (append)
  2  · Schema evolution: ADD COLUMN currency
  3  · Schema evolution: ADD COLUMN region
  4  · Append enriched batch            → fills new columns
  5  · MERGE INTO (upsert)             → correct amounts + insert
  6  · Row-level DELETE                 → purge micro-transactions
  7  · Table properties update          → custom tags
  8  · Nessie branch lifecycle          → dev → main merge
  9  · Snapshot inspection & time-travel (read-only)

Prerequisites
-------------
  - port-forward nessie-service   19120:19120
  - port-forward minio-service     9000:9000
  - The table `sales_transactions` should exist (created by deployIceberg.py).
    If it doesn't, step 1 will bootstrap it automatically.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Callable

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
CATALOG_NAME   = "nessie_catalog"
NAMESPACE      = "default"
TABLE_NAME     = "sales_transactions"
NESSIE_URI     = "http://localhost:19120/iceberg"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = "admin"
MINIO_SECRET   = "password"
WAREHOUSE      = "s3://warehouse/"

FULL_TABLE = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}"

STATE_FILE = Path(__file__).with_name("evolution_state.json")


# ─────────────────────────────────────────────────────────────────────────────
# State management
# ─────────────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"completed_steps": []}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def mark_done(state: dict, step_num: int) -> None:
    if step_num not in state["completed_steps"]:
        state["completed_steps"].append(step_num)
    save_state(state)


def reset_state() -> None:
    if STATE_FILE.exists():
        STATE_FILE.unlink()
    print(f"  ✔ State reset. {STATE_FILE.name} removed.")


# ─────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────────────────────────────────────

def banner(step: int, title: str) -> None:
    line = "═" * 70
    print(f"\n{line}")
    print(f"  STEP {step}  ·  {title}")
    print(f"{line}\n")


def has_column(spark: SparkSession, table: str, col: str) -> bool:
    return col in spark.table(table).columns


def show_snapshot_history(spark: SparkSession, full_table: str) -> None:
    print(f"  ↳ Snapshot history for {full_table}:")
    try:
        spark.read \
            .format("iceberg") \
            .load(f"{full_table}.history") \
            .select("made_current_at", "snapshot_id", "is_current_ancestor") \
            .show(truncate=False)
    except Exception as exc:
        print(f"  [WARN] Could not read history: {exc}")


def show_schema(spark: SparkSession, full_table: str) -> None:
    print(f"  ↳ Current schema of {full_table}:")
    spark.table(full_table).printSchema()


def show_table_properties(spark: SparkSession, full_table: str) -> None:
    print(f"  ↳ Table properties:")
    try:
        spark.sql(f"SHOW TBLPROPERTIES {full_table}").show(truncate=False)
    except Exception as exc:
        print(f"  [WARN] Could not read properties: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Spark session factory
# ─────────────────────────────────────────────────────────────────────────────

def build_spark(catalog: str = CATALOG_NAME, ref: str = "main") -> SparkSession:
    iceberg_pkg = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
        "org.apache.iceberg:iceberg-aws-bundle:1.6.1"
    )

    conf = SparkConf()
    conf.set("spark.jars.packages", iceberg_pkg)
    conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    conf.set(f"spark.sql.catalog.{catalog}.uri", NESSIE_URI)
    conf.set(f"spark.sql.catalog.{catalog}.ref", ref)

    conf.set(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set(f"spark.sql.catalog.{catalog}.warehouse", WAREHOUSE)
    conf.set(f"spark.sql.catalog.{catalog}.s3.endpoint", MINIO_ENDPOINT)
    conf.set(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
    conf.set(f"spark.sql.catalog.{catalog}.s3.access-key-id", MINIO_ACCESS)
    conf.set(f"spark.sql.catalog.{catalog}.s3.secret-access-key", MINIO_SECRET)
    conf.set(f"spark.sql.catalog.{catalog}.client.region", "us-east-1")

    return (
        SparkSession.builder
        .config(conf=conf)
        .appName(f"IcebergEvolution_{ref}")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Evolution steps  (each is idempotent)
# ─────────────────────────────────────────────────────────────────────────────

def ensure_table_exists(spark: SparkSession) -> None:
    try:
        spark.table(FULL_TABLE)
        print(f"  ✔ Table {FULL_TABLE} already exists.\n")
    except Exception:
        print(f"  ! Table not found — bootstrapping {FULL_TABLE} …")
        base_schema = StructType([
            StructField("tx_id",        StringType(),    False),
            StructField("client_id",    IntegerType(),   True),
            StructField("amount",       DoubleType(),    True),
            StructField("category",     StringType(),    True),
            StructField("tx_ts",        TimestampType(), True),
            StructField("ingestion_ts", TimestampType(), True),
        ])
        seed_data = [
            ("TX001", 101,  450.50, "Electronics", datetime(2023, 10, 21, 10, 0),  datetime.now()),
            ("TX002", 102,   20.00, "Food",         datetime(2023, 10, 21, 11, 30), datetime.now()),
            ("TX003", 101,  999.99, "Electronics",  datetime(2023, 10, 21, 12, 0),  datetime.now()),
            ("TX004", 103,    5.50, "Transport",    datetime(2023, 10, 22,  8, 15), datetime.now()),
            ("TX005", 102,  120.00, "Food",         datetime(2023, 10, 22, 20, 45), datetime.now()),
        ]
        spark.createDataFrame(seed_data, base_schema) \
            .writeTo(FULL_TABLE) \
            .partitionedBy(F.days("tx_ts")) \
            .createOrReplace()
        print("  ✔ Bootstrap complete.\n")


def step1_incremental_append(spark: SparkSession) -> None:
    """Append a new micro-batch of raw transactions (TX006–TX010)."""
    banner(1, "Incremental data append")

    new_rows = [
        ("TX006", 104,  310.00, "Health",       datetime(2023, 10, 23, 9, 10),  datetime.now()),
        ("TX007", 101,   88.50, "Books",         datetime(2023, 10, 23, 14, 5),  datetime.now()),
        ("TX008", 105, 2200.00, "Electronics",  datetime(2023, 10, 23, 18, 45), datetime.now()),
        ("TX009", 103,   15.00, "Transport",    datetime(2023, 10, 24,  7, 0),  datetime.now()),
        ("TX010", 102,   45.75, "Food",          datetime(2023, 10, 24, 13, 20), datetime.now()),
    ]

    existing_schema = spark.table(FULL_TABLE).schema
    base_cols = ["tx_id", "client_id", "amount", "category", "tx_ts", "ingestion_ts"]
    df_batch2 = spark.createDataFrame(
        [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in new_rows],
        spark.table(FULL_TABLE).select(base_cols).schema,
    )

    # Pad any extra columns (currency, region) that may already exist
    for col in existing_schema.fieldNames():
        if col not in base_cols:
            df_batch2 = df_batch2.withColumn(col, F.lit(None).cast("string"))

    df_batch2.writeTo(FULL_TABLE).append()
    print(f"  ✔ Appended {len(new_rows)} rows.\n")

    spark.table(FULL_TABLE).groupBy("category").count().orderBy("category").show()
    show_snapshot_history(spark, FULL_TABLE)


def step2_add_column_currency(spark: SparkSession) -> None:
    """Schema evolution: add nullable `currency` STRING column."""
    banner(2, "Schema evolution: ADD COLUMN currency")

    if has_column(spark, FULL_TABLE, "currency"):
        print("  ↷ Column `currency` already exists — skipping ALTER TABLE.\n")
    else:
        spark.sql(f"ALTER TABLE {FULL_TABLE} ADD COLUMN currency STRING")
        print("  ✔ Column `currency` added.\n")

    show_schema(spark, FULL_TABLE)
    show_snapshot_history(spark, FULL_TABLE)


def step3_add_column_region(spark: SparkSession) -> None:
    """Schema evolution: add nullable `region` STRING column."""
    banner(3, "Schema evolution: ADD COLUMN region")

    if has_column(spark, FULL_TABLE, "region"):
        print("  ↷ Column `region` already exists — skipping ALTER TABLE.\n")
    else:
        spark.sql(f"ALTER TABLE {FULL_TABLE} ADD COLUMN region STRING")
        print("  ✔ Column `region` added.\n")

    show_schema(spark, FULL_TABLE)
    show_snapshot_history(spark, FULL_TABLE)


def step4_append_enriched_batch(spark: SparkSession) -> None:
    """Append TX011–TX015 with currency and region populated."""
    banner(4, "Append enriched batch (fills currency + region)")

    enriched_schema = spark.table(FULL_TABLE).schema
    enriched_rows = [
        ("TX011", 101,  500.00, "Electronics", datetime(2023, 10, 25, 10, 0),  datetime.now(), "USD", "EMEA"),
        ("TX012", 106,   75.00, "Health",       datetime(2023, 10, 25, 11, 0),  datetime.now(), "EUR", "EMEA"),
        ("TX013", 107, 1800.00, "Electronics",  datetime(2023, 10, 25, 15, 30), datetime.now(), "USD", "AMER"),
        ("TX014", 104,   30.00, "Transport",    datetime(2023, 10, 26,  8, 0),  datetime.now(), "EUR", "EMEA"),
        ("TX015", 105,  620.00, "Books",         datetime(2023, 10, 26, 17, 45), datetime.now(), "GBP", "EMEA"),
    ]

    df_enriched = spark.createDataFrame(enriched_rows, enriched_schema)
    df_enriched.writeTo(FULL_TABLE).append()
    print(f"  ✔ Appended {len(enriched_rows)} enriched rows.\n")

    print("  ↳ Sample rows (showing new columns):")
    spark.table(FULL_TABLE).select(
        "tx_id", "amount", "category", "currency", "region", "tx_ts"
    ).orderBy(F.desc("tx_ts")).show(5)

    show_snapshot_history(spark, FULL_TABLE)


def step5_merge_upsert(spark: SparkSession) -> None:
    """MERGE INTO: correct TX001/TX003 amounts and insert TX016."""
    banner(5, "MERGE INTO — upsert (correct amounts + insert TX016)")

    corrections_data = [
        ("TX001", 101,  399.99, "Electronics", datetime(2023, 10, 21, 10, 0),  datetime.now(), "USD", "EMEA"),
        ("TX003", 101,  850.00, "Electronics",  datetime(2023, 10, 21, 12, 0),  datetime.now(), "USD", "EMEA"),
        ("TX016", 108,  250.00, "Health",        datetime(2023, 10, 27, 10, 0),  datetime.now(), "USD", "AMER"),
    ]

    enriched_schema = spark.table(FULL_TABLE).schema
    df_corrections = spark.createDataFrame(corrections_data, enriched_schema)
    df_corrections.createOrReplaceTempView("corrections_source")

    spark.sql(f"""
        MERGE INTO {FULL_TABLE} AS target
        USING corrections_source AS source
        ON target.tx_id = source.tx_id
        WHEN MATCHED THEN
            UPDATE SET
                target.amount       = source.amount,
                target.currency     = source.currency,
                target.region       = source.region,
                target.ingestion_ts = source.ingestion_ts
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    print("  ✔ MERGE completed: TX001 & TX003 corrected, TX016 inserted.\n")

    print("  ↳ Verification — TX001, TX003, TX016:")
    spark.table(FULL_TABLE) \
        .filter(F.col("tx_id").isin("TX001", "TX003", "TX016")) \
        .select("tx_id", "amount", "currency", "region") \
        .show()

    show_snapshot_history(spark, FULL_TABLE)


def step6_delete_rows(spark: SparkSession) -> None:
    """Row-level DELETE: purge Transport transactions below €10."""
    banner(6, "Row-level DELETE — purge micro-transactions in Transport")

    to_delete = spark.table(FULL_TABLE) \
        .filter((F.col("category") == "Transport") & (F.col("amount") < 10.0))

    count = to_delete.count()
    if count == 0:
        print("  ↷ No matching rows to delete — skipping.\n")
    else:
        print(f"  ↳ {count} row(s) to be deleted:")
        to_delete.show()
        spark.sql(f"""
            DELETE FROM {FULL_TABLE}
            WHERE category = 'Transport'
              AND amount   < 10.0
        """)
        print("  ✔ Rows deleted.\n")

    print("  ↳ Transport rows remaining:")
    spark.table(FULL_TABLE).filter(F.col("category") == "Transport").show()
    show_snapshot_history(spark, FULL_TABLE)


def step7_update_table_properties(spark: SparkSession) -> None:
    """Set custom Iceberg table properties (business metadata)."""
    banner(7, "Table properties update")

    props = {
        "team.owner"        : "data-platform",
        "domain"            : "finance",
        "pii.level"         : "low",
        "sla.freshness"     : "hourly",
        "write.delete.mode" : "merge-on-read",
        "write.update.mode" : "merge-on-read",
        "write.merge.mode"  : "merge-on-read",
    }

    set_clause = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    spark.sql(f"ALTER TABLE {FULL_TABLE} SET TBLPROPERTIES ({set_clause})")
    print(f"  ✔ Applied {len(props)} custom properties.\n")

    show_table_properties(spark, FULL_TABLE)


def step8_nessie_branch_lifecycle(spark: SparkSession) -> None:
    """Nessie branch workflow: create dev branch, add loyalty_points table, merge to main.

    Uses Nessie REST API v1 for branch management — it is simpler, stable across all
    Nessie versions, and confirmed available (minSupportedApiVersion=1).

    v1 reference endpoints:
      GET  /api/v1/trees/tree/{name}              → get branch/tag by name
      POST /api/v1/trees/branch                   → create branch (hash + ref as query params)
      POST /api/v1/trees/branch/{name}/merge      → merge source branch into named branch
    """
    banner(8, "Nessie branch lifecycle: dev → main merge")

    import requests

    NESSIE_V1   = "http://localhost:19120/api/v1"
    BRANCH_NAME = "dev-add-loyalty-table"

    # ── 8a. Fetch current HEAD of main (v1 response: {"type","name","hash"}) ─
    print("  8a. Fetching HEAD of 'main' …")
    resp = requests.get(f"{NESSIE_V1}/trees/tree/main")
    resp.raise_for_status()
    main_hash = resp.json()["hash"]
    print(f"       main HEAD = {main_hash[:16]}…\n")

    # ── 8b. Create branch from main HEAD ────────────────────────────────────
    # Spec §createReference: POST /v1/trees/tree?sourceRefName={source}
    # body: {"type": "BRANCH", "name": "<new>", "hash": "<hash-on-source>"}
    # 'sourceRefName' is required when 'hash' is provided.
    print(f"  8b. Creating branch '{BRANCH_NAME}' …")
    resp = requests.post(
        f"{NESSIE_V1}/trees/tree",
        params={"sourceRefName": "main"},
        json={"type": "BRANCH", "name": BRANCH_NAME, "hash": main_hash},
    )
    if resp.status_code == 409:
        print("       Branch already exists — continuing.\n")
    else:
        resp.raise_for_status()
        print("       Branch created.\n")

    # ── 8c. Switch Spark to the dev branch and create loyalty_points ────────
    print(f"  8c. Working on '{BRANCH_NAME}': creating loyalty_points …")
    spark.stop()
    spark_dev = build_spark(ref=BRANCH_NAME)

    loyalty_table = f"{CATALOG_NAME}.{NAMESPACE}.loyalty_points"
    loyalty_data = [
        (101, 4500, "Gold",     datetime.now()),
        (102,  800, "Silver",   datetime.now()),
        (103,  200, "Bronze",   datetime.now()),
        (104, 1200, "Silver",   datetime.now()),
        (105, 9800, "Platinum", datetime.now()),
        (106,  350, "Bronze",   datetime.now()),
    ]
    loyalty_schema = StructType([
        StructField("client_id",  IntegerType(),   False),
        StructField("points",     IntegerType(),   True),
        StructField("tier",       StringType(),    True),
        StructField("updated_at", TimestampType(), True),
    ])

    spark_dev.createDataFrame(loyalty_data, loyalty_schema) \
        .writeTo(loyalty_table) \
        .createOrReplace()
    print(f"       loyalty_points created on '{BRANCH_NAME}'.\n")

    print("  ↳ loyalty_points on dev branch:")
    spark_dev.table(loyalty_table).show()
    spark_dev.stop()

    # ── 8d. Merge dev branch into main via Nessie v1 REST API ───────────────
    # POST /api/v1/trees/branch/main/merge?expectedHash={current-main-hash}
    # body: the source reference (branch to merge from)
    print(f"  8d. Merging '{BRANCH_NAME}' → main …")

    resp = requests.get(f"{NESSIE_V1}/trees/tree/{BRANCH_NAME}")
    resp.raise_for_status()
    dev_hash = resp.json()["hash"]

    # Re-fetch main hash — it may have advanced since 8a if Spark wrote to it
    resp = requests.get(f"{NESSIE_V1}/trees/tree/main")
    resp.raise_for_status()
    current_main_hash = resp.json()["hash"]

    # Spec §mergeRefIntoBranch: body must be the Merge schema
    # required fields: fromRefName (source branch) + fromHash (tip commit to merge)
    resp = requests.post(
        f"{NESSIE_V1}/trees/branch/main/merge",
        params={"expectedHash": current_main_hash},
        json={"fromRefName": BRANCH_NAME, "fromHash": dev_hash},
    )
    if resp.status_code in (200, 204):
        print("  ✔ Merge successful. loyalty_points is now on main.\n")
    else:
        print(f"  [WARN] Merge returned {resp.status_code}: {resp.text}\n")


def step9_time_travel_and_metadata(spark: SparkSession) -> None:
    """Read-only: snapshot history, time-travel, files and manifests metadata."""
    banner(9, "Snapshot inspection & time-travel")

    print("  ↳ Full snapshot history:")
    snapshots_df = (
        spark.read
        .format("iceberg")
        .load(f"{FULL_TABLE}.snapshots")
        .select("committed_at", "snapshot_id", "operation", "summary")
        .orderBy("committed_at")
    )
    snapshots_df.show(truncate=False)

    snapshot_ids = [row["snapshot_id"] for row in snapshots_df.collect()]
    if len(snapshot_ids) >= 2:
        old_snapshot = snapshot_ids[1]
        print(f"\n  ↳ Time-travel read at snapshot {old_snapshot} (2nd commit):")
        spark.read \
            .format("iceberg") \
            .option("snapshot-id", str(old_snapshot)) \
            .load(f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}") \
            .show(5)
    else:
        print("  [INFO] Not enough snapshots yet for time-travel demo.")

    print("  ↳ Current data files (first 5):")
    spark.read \
        .format("iceberg") \
        .load(f"{FULL_TABLE}.files") \
        .select("file_path", "record_count", "file_size_in_bytes") \
        .show(5, truncate=True)

    print("  ↳ Manifest list (first 5):")
    spark.read \
        .format("iceberg") \
        .load(f"{FULL_TABLE}.manifests") \
        .select("path", "added_snapshot_id", "added_files_count", "deleted_files_count") \
        .show(5, truncate=True)


# ─────────────────────────────────────────────────────────────────────────────
# Step registry  — add new steps here
# ─────────────────────────────────────────────────────────────────────────────

STEPS: list[tuple[int, str, Callable]] = [
    (1, "Incremental data append (TX006–TX010)",        step1_incremental_append),
    (2, "Schema evolution: ADD COLUMN currency",        step2_add_column_currency),
    (3, "Schema evolution: ADD COLUMN region",          step3_add_column_region),
    (4, "Append enriched batch (TX011–TX015)",          step4_append_enriched_batch),
    (5, "MERGE INTO — upsert TX001/TX003, insert TX016",step5_merge_upsert),
    (6, "Row-level DELETE — Transport < €10",           step6_delete_rows),
    (7, "Table properties update (business metadata)",  step7_update_table_properties),
    (8, "Nessie branch lifecycle: dev → main",          step8_nessie_branch_lifecycle),
    (9, "Snapshot inspection & time-travel (read-only)",step9_time_travel_and_metadata),
]

STEP_MAP = {num: (title, fn) for num, title, fn in STEPS}


# ─────────────────────────────────────────────────────────────────────────────
# CLI helpers
# ─────────────────────────────────────────────────────────────────────────────

def print_step_list(state: dict) -> None:
    done = set(state["completed_steps"])
    print("\n  Step  Status  Description")
    print("  ────  ──────  " + "─" * 50)
    for num, title, _ in STEPS:
        status = "✔ done " if num in done else "✗ pending"
        print(f"  {num:>4}  {status}  {title}")
    print()


def resolve_steps_to_run(args: argparse.Namespace, state: dict) -> list[int]:
    done = set(state["completed_steps"])

    if args.step is not None:
        return [args.step]

    if args.steps:
        return [int(s.strip()) for s in args.steps.split(",")]

    pending = [num for num, _, _ in STEPS if num not in done]
    if not pending:
        return []

    if args.all:
        return pending

    # default: next single pending step
    return [pending[0]]


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Iceberg evolution steps incrementally.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--step",  type=int, metavar="N",
                       help="Run step N unconditionally (ignores completed state)")
    group.add_argument("--steps", metavar="N,M,…",
                       help="Run a comma-separated list of steps")
    group.add_argument("--all",   action="store_true",
                       help="Run all remaining pending steps")
    group.add_argument("--list",  action="store_true",
                       help="Show step status and exit")
    group.add_argument("--reset", action="store_true",
                       help="Clear evolution_state.json and exit")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    state = load_state()

    if args.list:
        print_step_list(state)
        sys.exit(0)

    if args.reset:
        reset_state()
        sys.exit(0)

    steps_to_run = resolve_steps_to_run(args, state)

    if not steps_to_run:
        print("\n  All steps are already completed. Use --reset to start over,")
        print("  or --step N / --steps N,M to force specific steps.\n")
        print_step_list(state)
        sys.exit(0)

    print("=" * 70)
    print("  IcebergEvolution (incremental)")
    print(f"  Table  : {FULL_TABLE}")
    print(f"  Catalog: {CATALOG_NAME}  →  {NESSIE_URI}")
    print(f"  Storage: {WAREHOUSE}  ({MINIO_ENDPOINT})")
    print(f"  Running steps: {steps_to_run}")
    print("=" * 70)

    spark = build_spark(ref="main")

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")
        ensure_table_exists(spark)

        for step_num in steps_to_run:
            if step_num not in STEP_MAP:
                print(f"  [WARN] Step {step_num} not found in registry — skipping.")
                continue

            title, fn = STEP_MAP[step_num]

            # step8 restarts Spark internally; rebuild if needed afterwards
            fn(spark)

            if step_num == 8:
                try:
                    if not spark._jvm.SparkSession.active():
                        spark = build_spark(ref="main")
                except Exception:
                    spark = build_spark(ref="main")

            mark_done(state, step_num)

    finally:
        try:
            spark.stop()
        except Exception:
            pass
        print("\n" + "=" * 70)
        print_step_list(state)
        print("  Re-run your DataHub Iceberg ingestion recipe to see the changes.")
        print("=" * 70)
