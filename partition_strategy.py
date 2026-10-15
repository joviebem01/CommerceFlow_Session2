"""
partition_strategy.py — evidence for choosing ``state`` as the partition key.

The workload groups the 300,000-order working dataset by state and computes:
- order_count
- successful_orders
- revenue_total
- average order value (AOV)

The script evaluates business/grouping candidates and records the measured
distribution for the chosen key before the Spark experiment is run.
"""
from __future__ import annotations

import json
import sys
from typing import Any

import pandas as pd

import config as cfg
from load_and_join import build_working_dataset, load_frames


CANDIDATE_COLUMNS = [
    "state",
    "source",
    "country",
    "city",
    "category_normalized",
    "payment_method",
    "status_normalized",
    "quantity",
    cfg.CUSTOMER_KEY,
    cfg.PRODUCT_KEY,
]

IDENTIFIER_COLUMNS = {
    cfg.ORDER_KEY,
    cfg.CUSTOMER_KEY,
    cfg.PRODUCT_KEY,
    "event_id",
    "ticket_id",
    "session_id",
    "device_id",
}

MIN_DISTINCT = 2
MAX_DISTINCT_RATIO = 0.50


def owner_of(column: str) -> str:
    mapping = {
        "state": "CRM customer",
        "source": "CRM customer",
        "country": "CRM customer",
        "city": "CRM customer",
        "category_normalized": "product catalog",
        "payment_method": "orders",
        "status_normalized": "orders (cleaned)",
        "quantity": "orders",
        cfg.CUSTOMER_KEY: "orders/CRM/clickstream/support",
        cfg.PRODUCT_KEY: "orders/product catalog",
    }
    return mapping.get(column, "working dataset")


def evaluate_column(
    working: pd.DataFrame,
    column: str,
) -> dict[str, Any]:
    series = working[column]
    counts = series.value_counts(dropna=False)
    distinct = int(series.nunique(dropna=True))
    rejects: list[str] = []

    if column in IDENTIFIER_COLUMNS:
        rejects.append("identifier, not a business grouping attribute")
    if distinct < MIN_DISTINCT:
        rejects.append(f"only {distinct} distinct value(s)")
    if distinct > len(working) * MAX_DISTINCT_RATIO:
        rejects.append(f"{distinct:,} distinct values is near-unique")
    if series.isna().any():
        rejects.append(f"{int(series.isna().sum()):,} null values")

    non_null_counts = series.dropna().value_counts()
    if len(non_null_counts):
        skew = float(non_null_counts.max() / non_null_counts.min())
        minimum = int(non_null_counts.min())
        median = int(non_null_counts.median())
        maximum = int(non_null_counts.max())
    else:
        skew = 0.0
        minimum = median = maximum = 0

    return {
        "column": column,
        "source": owner_of(column),
        "distinct": distinct,
        "min": minimum,
        "median": median,
        "max": maximum,
        "skew_ratio": round(skew, 4),
        "viable": not rejects,
        "rejected_because": "; ".join(rejects),
    }


def main() -> int:
    cfg.banner("SESSION 1 — CommerceFlow PARTITIONING STRATEGY")
    frames = load_frames()
    working, join_report = build_working_dataset(frames, verbose=False)

    candidates = [
        evaluate_column(working, c)
        for c in CANDIDATE_COLUMNS
        if c in working.columns
    ]

    cfg.banner("CANDIDATE PARTITION KEYS")
    print(
        f"  {'column':<22}{'distinct':>10}{'min':>10}"
        f"{'median':>10}{'max':>10}{'skew':>9}  verdict"
    )
    print("  " + "-" * 94)
    for c in candidates:
        verdict = "VIABLE" if c["viable"] else c["rejected_because"]
        print(
            f"  {c['column']:<22}{c['distinct']:>10,}{c['min']:>10,}"
            f"{c['median']:>10,}{c['max']:>10,}"
            f"{c['skew_ratio']:>9.2f}  {verdict}"
        )

    chosen = next(
        c for c in candidates if c["column"] == cfg.PARTITION_KEY
    )
    if not chosen["viable"]:
        raise AssertionError(
            f"Configured partition key {cfg.PARTITION_KEY!r} is not viable: "
            f"{chosen['rejected_because']}"
        )

    cfg.banner(f"CHOSEN KEY — {cfg.PARTITION_KEY}")
    print(f"  owned by                : {chosen['source']}")
    print(f"  distinct values         : {chosen['distinct']}")
    print(
        f"  records/key             : min={chosen['min']:,} "
        f"median={chosen['median']:,} max={chosen['max']:,}"
    )
    print(f"  predicted skew ratio    : {chosen['skew_ratio']:.2f} : 1")

    workload = (
        f"Compute order_count, successful_orders, revenue_total, and AOV "
        f"per {cfg.PARTITION_KEY} across the {len(working):,}-row "
        f"order-grain working dataset."
    )

    cfg.banner("WORKLOAD DEFINITION")
    print("  " + workload)
    print("\n  output schema:")
    for col, dtype in {
        cfg.PARTITION_KEY: "string",
        "order_count": "int64",
        "successful_orders": "int64",
        "revenue_total": "double",
        "aov": "double",
    }.items():
        print(f"    {col:<20} {dtype}")

    report = {
        "chosen_key": cfg.PARTITION_KEY,
        "owned_by": chosen["source"],
        "prediction": {
            "distinct": chosen["distinct"],
            "min": chosen["min"],
            "median": chosen["median"],
            "max": chosen["max"],
            "skew_ratio": chosen["skew_ratio"],
        },
        "all_candidates": candidates,
        "workload": workload,
        "output_schema": {
            cfg.PARTITION_KEY: "string",
            "order_count": "int64",
            "successful_orders": "int64",
            "revenue_total": "double",
            "aov": "double",
        },
        "join": join_report,
    }
    cfg.OUT_PARTITION_STRATEGY.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )
    print(f"\nWrote {cfg.OUT_PARTITION_STRATEGY}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
