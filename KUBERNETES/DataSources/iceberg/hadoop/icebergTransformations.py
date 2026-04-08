"""
evolveIceberg.py
================
Simulates a realistic catalog evolution lifecycle on an existing Apache Iceberg
table managed by a Nessie REST catalog and stored in MinIO (S3-compatible).

Each step creates a distinct Nessie commit / Iceberg snapshot so that lineage
and schema changes are visible in Datahub after re-ingestion.

Evolution sequence
------------------
  STEP 1  · Incremental data append          → new snapshot (append)
  STEP 2  · Schema evolution: ADD COLUMN     → metadata commit  (currency STRING)
  STEP 3  · Schema evolution: ADD COLUMN     → metadata commit  (region   STRING)
  STEP 4  · Append enriched batch            → new snapshot (append, fills new cols)
  STEP 5  · MERGE INTO (upsert)              → new snapshot (overwrite-by-filter)
  STEP 6  · Row-level DELETE                 → new snapshot (delete)
  STEP 7  · Table properties update          → metadata commit  (custom tags)
  STEP 8  · Nessie branch: dev lifecycle     → commits on branch 'dev', then merge
  STEP 9  · Snapshot inspection & time-travel

Prerequisites
-------------
  - port-forward nessie-service   19120:19120
  - port-forward minio-service     9000:9000
  - The table `sales_transactions` must already exist in catalog `nessie_catalog`
    (created by deployIceberg.py).  If it doesn't, this script creates it first.
"""

import os
import time
from datetime import datetime

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)


# ─────────────────────────────────────────────────────────────────────────────
# Constants — adjust to match your cluster
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


# ─────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────────────────────────────────────

def banner(step: int, title: str) -> None:
    line = "═" * 70
    print(f"\n{line}")
    print(f"  STEP {step}  ·  {title}")
    print(f"{line}\n")


def show_snapshot_history(spark: SparkSession, full_table: str) -> None:
    """Print the Iceberg snapshot log for the given table."""
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
    """Print the current schema of the table."""
    print(f"  ↳ Current schema of {full_table}:")
    spark.table(full_table).printSchema()


def show_table_properties(spark: SparkSession, full_table: str) -> None:
    """Print the current Iceberg table properties."""
    print(f"  ↳ Table properties:")
    try:
        spark.sql(f"SHOW TBLPROPERTIES {full_table}").show(truncate=False)
    except Exception as exc:
        print(f"  [WARN] Could not read properties: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Spark session factory
# ─────────────────────────────────────────────────────────────────────────────

def build_spark(catalog: str = CATALOG_NAME, ref: str = "main") -> SparkSession:
    """
    Build a SparkSession wired to the Nessie REST catalog and MinIO.
    `ref` controls which Nessie branch/tag Spark will read/write from.
    """
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

    # Nessie REST catalog
    conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    conf.set(f"spark.sql.catalog.{catalog}.uri", NESSIE_URI)
    conf.set(f"spark.sql.catalog.{catalog}.ref", ref)             # ← branch/tag

    # MinIO / S3FileIO
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
# Evolution steps
# ─────────────────────────────────────────────────────────────────────────────

def ensure_table_exists(spark: SparkSession) -> None:
    """
    Safety net: if deployIceberg.py has not been run yet, bootstrap the table
    with the base schema so the evolution steps have something to operate on.
    """
    try:
        spark.table(FULL_TABLE)
        print(f"  ✔ Table {FULL_TABLE} already exists — skipping bootstrap.\n")
    except Exception:
        print(f"  ! Table not found. Creating {FULL_TABLE} with base schema …")
        base_schema = StructType([
            StructField("tx_id",        StringType(),    False),
            StructField("client_id",    IntegerType(),   True),
            StructField("amount",       DoubleType(),    True),
            StructField("category",     StringType(),    True),
            StructField("tx_ts",        TimestampType(), True),
            StructField("ingestion_ts", TimestampType(), True),
        ])
        seed_data = [
            ("TX001", 101,  450.50, "Electronics", datetime(2023, 10, 21, 10, 0), datetime.now()),
            ("TX002", 102,   20.00, "Food",         datetime(2023, 10, 21, 11, 30), datetime.now()),
            ("TX003", 101,  999.99, "Electronics",  datetime(2023, 10, 21, 12, 0), datetime.now()),
            ("TX004", 103,    5.50, "Transport",    datetime(2023, 10, 22,  8, 15), datetime.now()),
            ("TX005", 102,  120.00, "Food",         datetime(2023, 10, 22, 20, 45), datetime.now()),
        ]
        spark.createDataFrame(seed_data, base_schema) \
            .writeTo(FULL_TABLE) \
            .partitionedBy(F.days("tx_ts")) \
            .createOrReplace()
        print("  ✔ Bootstrap complete.\n")


def step1_incremental_append(spark: SparkSession) -> None:
    """
    STEP 1 — Append a new micro-batch of raw transactions.
    Produces: 1 new Iceberg snapshot (operation = append).
    """
    banner(1, "Incremental data append")

    new_rows = [
        ("TX006", 104,  310.00, "Health",       datetime(2023, 10, 23, 9, 10),  datetime.now()),
        ("TX007", 101,   88.50, "Books",         datetime(2023, 10, 23, 14, 5),  datetime.now()),
        ("TX008", 105, 2200.00, "Electronics",  datetime(2023, 10, 23, 18, 45), datetime.now()),
        ("TX009", 103,   15.00, "Transport",    datetime(2023, 10, 24,  7, 0),  datetime.now()),
        ("TX010", 102,   45.75, "Food",          datetime(2023, 10, 24, 13, 20), datetime.now()),
    ]

    # Match the existing table schema (no currency/region yet)
    existing_schema = spark.table(FULL_TABLE).schema
    df_batch2 = spark.createDataFrame(
        [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in new_rows],
        existing_schema,
    )

    df_batch2.writeTo(FULL_TABLE).append()
    print(f"  ✔ Appended {len(new_rows)} rows.\n")

    spark.table(FULL_TABLE).groupBy("category").count().orderBy("category").show()
    show_snapshot_history(spark, FULL_TABLE)


def step2_add_column_currency(spark: SparkSession) -> None:
    """
    STEP 2 — Schema evolution: add a nullable `currency` column (STRING).
    Produces: 1 new Nessie metadata commit (no data files written).
    Iceberg tracks schema changes as first-class versioned events.
    """
    banner(2, "Schema evolution: ADD COLUMN currency")

    spark.sql(f"ALTER TABLE {FULL_TABLE} ADD COLUMN currency STRING")
    print("  ✔ Column `currency` added (nullable STRING, default = NULL for old rows).\n")

    show_schema(spark, FULL_TABLE)
    show_snapshot_history(spark, FULL_TABLE)


def step3_add_column_region(spark: SparkSession) -> None:
    """
    STEP 3 — Schema evolution: add a `region` column (STRING).
    Produces: 1 more metadata commit.
    Adding columns in separate commits makes history granular & auditable.
    """
    banner(3, "Schema evolution: ADD COLUMN region")

    spark.sql(f"ALTER TABLE {FULL_TABLE} ADD COLUMN region STRING")
    print("  ✔ Column `region` added (nullable STRING, default = NULL for old rows).\n")

    show_schema(spark, FULL_TABLE)
    show_snapshot_history(spark, FULL_TABLE)


def step4_append_enriched_batch(spark: SparkSession) -> None:
    """
    STEP 4 — Append a batch that populates the new columns.
    Produces: 1 new Iceberg snapshot (operation = append).
    Old rows keep NULL for currency/region — Iceberg handles schema evolution
    transparently, no rewrite of existing files needed.
    """
    banner(4, "Append enriched batch (fills currency + region)")

    enriched_schema = spark.table(FULL_TABLE).schema  # now includes all columns

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
    """
    STEP 5 — MERGE INTO (upsert): correct TX001/TX003 amounts (detected as
    data-quality issues) and insert TX016 as a new row in one atomic operation.
    Produces: 1 new Iceberg snapshot (operation = overwrite).

    MERGE INTO is a Spark Iceberg SQL extension — it requires IcebergSparkSessionExtensions.
    """
    banner(5, "MERGE INTO — upsert (correct amounts + insert new row)")

    # Build the source DataFrame for the MERGE
    corrections_data = [
        ("TX001", 101,  399.99, "Electronics", datetime(2023, 10, 21, 10, 0),  datetime.now(), "USD", "EMEA"),
        ("TX003", 101,  850.00, "Electronics",  datetime(2023, 10, 21, 12, 0),  datetime.now(), "USD", "EMEA"),
        ("TX016", 108,  250.00, "Health",        datetime(2023, 10, 27, 10, 0),  datetime.now(), "USD", "AMER"),
    ]

    # Use the current enriched schema
    enriched_schema = spark.table(FULL_TABLE).schema
    df_corrections = spark.createDataFrame(corrections_data, enriched_schema)

    # Register as a temp view for SQL MERGE syntax
    df_corrections.createOrReplaceTempView("corrections_source")

    merge_sql = f"""
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
    """
    spark.sql(merge_sql)
    print("  ✔ MERGE completed: TX001 & TX003 corrected, TX016 inserted.\n")

    print("  ↳ Verification — TX001, TX003, TX016:")
    spark.table(FULL_TABLE) \
        .filter(F.col("tx_id").isin("TX001", "TX003", "TX016")) \
        .select("tx_id", "amount", "currency", "region") \
        .show()

    show_snapshot_history(spark, FULL_TABLE)


def step6_delete_rows(spark: SparkSession) -> None:
    """
    STEP 6 — Row-level DELETE: remove all Transport transactions below €10
    (noise / test transactions identified by data-quality team).
    Produces: 1 new Iceberg snapshot (operation = delete).

    Row-level deletes in Iceberg v2 tables write delete files without
    rewriting the entire data file — copy-on-write or merge-on-read depending
    on the table's delete mode property.
    """
    banner(6, "Row-level DELETE — purge micro-transactions in Transport")

    # Show what we are about to delete
    print("  ↳ Rows to be deleted:")
    spark.table(FULL_TABLE) \
        .filter((F.col("category") == "Transport") & (F.col("amount") < 10.0)) \
        .show()

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
    """
    STEP 7 — Set custom Iceberg table properties to capture business metadata.
    Properties are visible in Datahub as dataset custom properties.
    Produces: 1 metadata commit (no snapshot, no data file written).
    """
    banner(7, "Table properties update")

    props = {
        "team.owner"          : "data-platform",
        "domain"              : "finance",
        "pii.level"           : "low",
        "sla.freshness"       : "hourly",
        "write.delete.mode"   : "merge-on-read",    # enable MOR deletes
        "write.update.mode"   : "merge-on-read",    # enable MOR updates
        "write.merge.mode"    : "merge-on-read",
    }

    set_clause = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    spark.sql(f"ALTER TABLE {FULL_TABLE} SET TBLPROPERTIES ({set_clause})")
    print(f"  ✔ Applied {len(props)} custom properties.\n")

    show_table_properties(spark, FULL_TABLE)


def step8_nessie_branch_lifecycle(spark: SparkSession) -> None:
    """
    STEP 8 — Nessie branch-based development workflow:
      8a. Create branch `dev/add-loyalty-table`
      8b. On that branch: create a new `loyalty_points` table
      8c. On that branch: append an initial batch
      8d. Merge branch into `main` via the Nessie REST API
          (Spark cannot merge branches natively; we use the `requests` library)

    This simulates a typical DataOps pattern where schema/table changes
    are developed on a branch and promoted to main only when validated.
    """
    banner(8, "Nessie branch lifecycle: dev → main merge")

    import requests

    NESSIE_REST = "http://localhost:19120/api/v2"
    BRANCH_NAME = "dev/add-loyalty-table"

    # ── 8a. Fetch current HEAD of main ──────────────────────────────────────
    print(f"  8a. Fetching HEAD of 'main' …")
    resp = requests.get(f"{NESSIE_REST}/branches/main")
    resp.raise_for_status()
    main_hash = resp.json()["reference"]["hash"]
    print(f"       main HEAD = {main_hash[:16]}…\n")

    # ── 8b. Create branch from main HEAD ────────────────────────────────────
    print(f"  8b. Creating branch '{BRANCH_NAME}' …")
    create_payload = {
        "type"     : "BRANCH",
        "name"     : BRANCH_NAME,
        "reference": {"type": "BRANCH", "name": "main", "hash": main_hash},
    }
    resp = requests.post(f"{NESSIE_REST}/branches", json=create_payload)
    if resp.status_code == 409:
        print(f"       Branch already exists — continuing.\n")
    else:
        resp.raise_for_status()
        print(f"       Branch created.\n")

    # ── 8c. Switch Spark to the dev branch and create loyalty_points ────────
    print(f"  8c. Working on branch '{BRANCH_NAME}': creating loyalty_points …")
    spark.stop()                                          # restart Spark on dev branch
    spark_dev = build_spark(ref=BRANCH_NAME)

    loyalty_table = f"{CATALOG_NAME}.{NAMESPACE}.loyalty_points"

    loyalty_data = [
        (101, 4500,  "Gold",   datetime.now()),
        (102,  800,  "Silver", datetime.now()),
        (103,  200,  "Bronze", datetime.now()),
        (104, 1200,  "Silver", datetime.now()),
        (105, 9800,  "Platinum", datetime.now()),
        (106,  350,  "Bronze", datetime.now()),
    ]
    loyalty_schema = StructType([
        StructField("client_id",     IntegerType(),   False),
        StructField("points",        IntegerType(),   True),
        StructField("tier",          StringType(),    True),
        StructField("updated_at",    TimestampType(), True),
    ])

    spark.createDataFrame(loyalty_data, loyalty_schema) \
        .writeTo(loyalty_table) \
        .createOrReplace()
    print(f"       loyalty_points created on '{BRANCH_NAME}'.\n")

    # Verify on branch
    print("  ↳ loyalty_points on dev branch:")
    spark_dev.table(loyalty_table).show()
    spark_dev.stop()

    # ── 8d. Merge dev branch into main via Nessie REST API ──────────────────
    print(f"  8d. Merging '{BRANCH_NAME}' → main …")
    resp  = requests.get(f"{NESSIE_REST}/branches/{BRANCH_NAME}")
    resp.raise_for_status()
    dev_hash = resp.json()["reference"]["hash"]

    merge_payload = {
        "message"           : f"Merge {BRANCH_NAME}: add loyalty_points table",
        "fromRefName"       : BRANCH_NAME,
        "fromHash"          : dev_hash,
        "defaultMergeMode"  : "NORMAL",
    }
    resp = requests.post(f"{NESSIE_REST}/branches/main/merge", json=merge_payload)
    if resp.status_code in (200, 204):
        print(f"  ✔ Merge successful. loyalty_points is now on main.\n")
    else:
        print(f"  [WARN] Merge returned {resp.status_code}: {resp.text}\n")


def step9_time_travel_and_metadata(spark: SparkSession) -> None:
    """
    STEP 9 — Demonstrate Iceberg time-travel and metadata table queries.
    These are read-only; no new snapshots are created.
    """
    banner(9, "Snapshot inspection & time-travel")

    # Full snapshot history
    print("  ↳ Full snapshot history:")
    snapshots_df = (
        spark.read
        .format("iceberg")
        .load(f"{FULL_TABLE}.snapshots")
        .select("committed_at", "snapshot_id", "operation", "summary")
        .orderBy("committed_at")
    )
    snapshots_df.show(truncate=False)

    # Grab the second snapshot's ID for time-travel demo
    snapshot_ids = [row["snapshot_id"] for row in snapshots_df.collect()]
    if len(snapshot_ids) >= 2:
        old_snapshot = snapshot_ids[1]
        print(f"\n  ↳ Time-travel read at snapshot {old_snapshot} (the 2nd commit):")
        spark.read \
            .format("iceberg") \
            .option("snapshot-id", str(old_snapshot)) \
            .load(f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}") \
            .show(5)
    else:
        print("  [INFO] Not enough snapshots yet for time-travel demo.")

    # Files metadata table
    print("  ↳ Current data files metadata (first 5 rows):")
    spark.read \
        .format("iceberg") \
        .load(f"{FULL_TABLE}.files") \
        .select("file_path", "record_count", "file_size_in_bytes") \
        .show(5, truncate=True)

    # Manifests
    print("  ↳ Manifest list (first 5 rows):")
    spark.read \
        .format("iceberg") \
        .load(f"{FULL_TABLE}.manifests") \
        .select("path", "added_snapshot_id", "added_files_count", "deleted_files_count") \
        .show(5, truncate=True)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("  IcebergEvolution — Catalog lifecycle simulation")
    print(f"  Table  : {FULL_TABLE}")
    print(f"  Catalog: {CATALOG_NAME}  →  {NESSIE_URI}")
    print(f"  Storage: {WAREHOUSE}  ({MINIO_ENDPOINT})")
    print("=" * 70)

    spark = build_spark(ref="main")

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")

        # Safety net: bootstrap table if deployIceberg.py hasn't been run
        ensure_table_exists(spark)

        # ── Schema + data evolution ──────────────────────────────────────────
        step1_incremental_append(spark)
        step2_add_column_currency(spark)
        step3_add_column_region(spark)
        step4_append_enriched_batch(spark)
        step5_merge_upsert(spark)
        step6_delete_rows(spark)
        step7_update_table_properties(spark)

        # ── Nessie branch lifecycle ──────────────────────────────────────────
        # step8 restarts the Spark session internally; we pass a reference
        # so the function can close and recreate it on the dev branch.
        try:
            step8_nessie_branch_lifecycle(spark)
        except Exception as exc:
            print(f"  [WARN] step8 skipped (requests not available or Nessie error): {exc}")

        # Restart on main for the final inspection
        if not spark._jvm.SparkSession.active():
            spark = build_spark(ref="main")

        # ── Metadata inspection ──────────────────────────────────────────────
        step9_time_travel_and_metadata(spark)

    finally:
        try:
            spark.stop()
        except Exception:
            pass
        print("\n" + "=" * 70)
        print("  Evolution simulation complete.")
        print("  Re-run your Datahub Iceberg ingestion recipe to see the changes.")
        print("=" * 70)