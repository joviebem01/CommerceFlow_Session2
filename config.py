"""
config.py — shared configuration for CommerceFlow Session 1.

This project is adapted to the five uploaded e-commerce CSV files:
- clickstream_500k_events.csv
- crm_50000_customers_dirty_v3.csv
- orders_300k_dirty.csv
- product_catalog_dirty_30pct.csv
- support_tickets_30000_dirty.csv

Every pipeline script imports this module so paths, keys, workload fields,
parallelism settings, and output locations stay consistent.
"""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SESSION_DIR = REPO_ROOT
DATA_DIR = Path(os.environ.get("CommerceFlow_DATA_DIR", SESSION_DIR / "Datasets")).resolve()
RESULTS_DIR = SESSION_DIR / "results"
DOCS_DIR = SESSION_DIR / "docs"
ARCH_DIR = SESSION_DIR / "architecture"

for directory in (RESULTS_DIR, DOCS_DIR, ARCH_DIR):
    directory.mkdir(parents=True, exist_ok=True)

FILES = {
    "clickstream": {
        "file": "clickstream_500k_events.csv",
        "role": "Event",
        "pk": "event_id",
    },
    "customers": {
        "file": "crm_50000_customers_dirty_v3.csv",
        "role": "Entity",
        "pk": "customer_id",
    },
    "orders": {
        "file": "orders_300k_dirty.csv",
        "role": "Event",
        "pk": "order_id",
    },
    "products": {
        "file": "product_catalog_dirty_30pct.csv",
        "role": "Entity",
        "pk": "product_id",
    },
    "support": {
        "file": "support_tickets_30000_dirty.csv",
        "role": "Event",
        "pk": "ticket_id",
    },
}


def path_for(name: str) -> Path:
    """Resolve an input CSV.

    The exact clean filename is preferred. If the file was downloaded more than
    once and Windows/browser added a suffix such as ``(2)``, a single matching
    variant is accepted automatically.
    """
    spec = FILES[name]
    exact = DATA_DIR / spec["file"]
    if exact.exists():
        return exact

    stem = exact.stem
    candidates = sorted(
        DATA_DIR.glob(f"{stem}*.csv"),
        key=lambda p: (len(p.name), p.name.lower()),
    )
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        # Prefer a duplicate-download pattern such as name(2).csv.
        numbered = [p for p in candidates if p.stem.startswith(stem + "(")]
        if len(numbered) == 1:
            return numbered[0]

    raise FileNotFoundError(
        f"Could not resolve {spec['file']} in {DATA_DIR}. "
        f"Put the five CSVs in the Datasets folder or set CommerceFlow_DATA_DIR."
    )


# Logical keys and workload
ORDER_KEY = "order_id"
CUSTOMER_KEY = "customer_id"
PRODUCT_KEY = "product_id"
CLICKSTREAM_KEY = "event_id"
SUPPORT_KEY = "ticket_id"

PARTITION_KEY = "state"
EVENT_TIME_FIELD = "order_date"

# The derived monetization measure used by both pandas and Spark.
# A row produces revenue only when the normalized status is "success"
# and the cleaned order amount is positive.
METRIC_FIELD = "revenue_amount"

PARTITION_SETTINGS = (2, 4, 8)
CHOSEN_PARTITIONS = 4
BENCHMARK_REPEATS = 3
BASELINE_REPEATS = 5
TOLERANCE = 1e-6

# Spark
SPARK_APP_NAME = "MIT261-Session1-CommerceFlow"
SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "4g"
SPARK_SHUFFLE_PARTITIONS = "8"


def build_spark():
    """Return the SparkSession used by every parallel script."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Outputs
OUT_PROFILE = RESULTS_DIR / "file_profile.json"
OUT_JOINED = RESULTS_DIR / "working_dataset.parquet"
OUT_PARTITION_STRATEGY = RESULTS_DIR / "partition_strategy.json"
OUT_BASELINE = RESULTS_DIR / "baseline_result.csv"
OUT_FINAL = RESULTS_DIR / "state_revenue.parquet"
OUT_VALIDATION = RESULTS_DIR / "validation_report.json"
OUT_BENCHMARK = RESULTS_DIR / "session1_benchmark.csv"
OUT_PARTITIONS = RESULTS_DIR / "partition_sizes.csv"

OUT_ENTITY_DIAGRAM = DOCS_DIR / "entity-model-session1.png"
OUT_UML_DIAGRAM = DOCS_DIR / "ecommerce_uml.png"
OUT_ARCH_DIAGRAM = ARCH_DIR / "architecture-session1.png"
OUT_SESSION2_ENTITY = DOCS_DIR / "entity-model-session2.png"
OUT_SESSION2_ARCH = ARCH_DIR / "architecture-session2.png"


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)
