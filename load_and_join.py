"""
load_and_join.py — build the CommerceFlow order-grain working dataset.

The five source files do NOT have the same grain:
- orders: one row per order
- CRM: one or more dirty copies per customer
- product catalog: one row per product
- clickstream: many events per customer, with anonymous rows allowed
- support: many tickets per customer

To preserve the 300,000-order grain, CRM is canonicalized to one row/customer,
and clickstream/support are reduced to one feature row/customer before joining.
Every merge uses validate="many_to_one" and a left join, then the final row
count is asserted to equal the original order count.
"""
from __future__ import annotations

import sys
import time
from typing import Any

import numpy as np
import pandas as pd

import config as cfg


def load_frames() -> dict[str, pd.DataFrame]:
    """Read all configured input files."""
    return {
        name: pd.read_csv(cfg.path_for(name), low_memory=False)
        for name in cfg.FILES
    }


def clean_numeric(series: pd.Series) -> pd.Series:
    """Parse dirty numeric text such as '1,200' while preserving nulls."""
    return pd.to_numeric(
        series.astype("string").str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )


def normalize_status(series: pd.Series) -> pd.Series:
    """Normalize the status variants present in the dirty orders file."""
    normalized = series.astype("string").str.strip().str.lower()
    return normalized.replace({
        "suc": "success",
        "ref": "refunded",
        "fail": "failed",
    })


def canonicalize_customers(customers: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Reduce dirty CRM copies to one row per customer.

    For the current dataset, duplicate customer IDs do not disagree on state,
    signup_date, source, city, country, or gender. We still assert state
    consistency because state is the partition key.
    """
    if customers[cfg.CUSTOMER_KEY].isna().any():
        raise ValueError("CRM has null customer_id values: cannot safely join")
    counts = customers[cfg.CUSTOMER_KEY].value_counts()
    duplicate_ids = counts[counts > 1].index

    dup = customers[customers[cfg.CUSTOMER_KEY].isin(duplicate_ids)]
    state_conflicts = 0
    if len(dup):
        state_conflicts = int(
            dup.groupby(cfg.CUSTOMER_KEY)[cfg.PARTITION_KEY]
            .nunique(dropna=False)
            .gt(1)
            .sum()
        )
    if state_conflicts:
        raise AssertionError(
            f"{state_conflicts} duplicated customer IDs disagree on "
            f"{cfg.PARTITION_KEY}; canonicalization is unsafe."
        )

    # Use fields relevant to the Session 1 analytics. Keeping the first copy is
    # safe for these fields because profiling verified state consistency.
    keep_cols = [
        cfg.CUSTOMER_KEY,
        "signup_date",
        "state",
        "country",
        "city",
        "source",
        "gender",
    ]
    keep_cols = [c for c in keep_cols if c in customers.columns]
    canonical = (
        customers[keep_cols]
        .drop_duplicates(cfg.CUSTOMER_KEY, keep="first")
        .copy()
    )

    return canonical, {
        "raw_rows": int(len(customers)),
        "canonical_rows": int(len(canonical)),
        "duplicate_rows_removed": int(
            customers.duplicated(cfg.CUSTOMER_KEY).sum()
        ),
        "duplicate_customer_ids": int(len(duplicate_ids)),
    }


def prepare_products(products: pd.DataFrame) -> pd.DataFrame:
    """Make product fields safe for joining without inventing category fixes."""
    out = products.copy()
    if out[cfg.PRODUCT_KEY].isna().any():
        raise ValueError("Product catalog has null product_id values")
    if out[cfg.PRODUCT_KEY].duplicated().any():
        raise AssertionError("product_id is not unique in the product catalog")

    out["product_price_clean"] = clean_numeric(out["price"])
    out["category_normalized"] = (
        out["category"].astype("string").str.strip().str.lower()
    )
    return out[
        [
            cfg.PRODUCT_KEY,
            "product_name",
            "category",
            "category_normalized",
            "product_price_clean",
        ]
    ]


def reduce_clickstream(clickstream: pd.DataFrame) -> pd.DataFrame:
    """Reduce many clickstream rows to one feature row per identified customer."""
    identified = clickstream[clickstream[cfg.CUSTOMER_KEY].notna()].copy()
    identified["timestamp_parsed"] = pd.to_datetime(
        identified["timestamp"],
        errors="coerce",
        format="mixed",
        utc=True,
    )

    reduced = (
        identified.groupby(cfg.CUSTOMER_KEY, as_index=False)
        .agg(
            clickstream_events=(cfg.CLICKSTREAM_KEY, "count"),
            clickstream_sessions=("session_id", "nunique"),
            clickstream_devices=("device_id", "nunique"),
            last_click_timestamp=("timestamp_parsed", "max"),
        )
    )
    return reduced


def reduce_support(support: pd.DataFrame) -> pd.DataFrame:
    """Reduce many support tickets to one feature row per customer."""
    work = support.copy()
    work["ticket_created_parsed"] = pd.to_datetime(
        work["ticket_created"],
        errors="coerce",
        format="mixed",
        utc=True,
    )
    work["resolution_time_hours_clean"] = clean_numeric(
        work["resolution_time_hours"]
    )
    work["unresolved_ticket"] = work["ticket_resolved"].isna().astype("int64")

    reduced = (
        work.groupby(cfg.CUSTOMER_KEY, as_index=False)
        .agg(
            support_tickets=(cfg.SUPPORT_KEY, "count"),
            avg_resolution_time_hours=("resolution_time_hours_clean", "mean"),
            unresolved_tickets=("unresolved_ticket", "sum"),
            last_ticket_created=("ticket_created_parsed", "max"),
        )
    )
    return reduced


def prepare_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """Normalize fields used by the monetization workload."""
    out = orders.copy()
    out["order_amount_clean"] = clean_numeric(out["order_amount"])
    out["status_normalized"] = normalize_status(out["status"])

    success = (
        out["status_normalized"].eq("success")
        & out["order_amount_clean"].gt(0)
    ).fillna(False)
    out["successful_order"] = success.astype("int64")
    out[cfg.METRIC_FIELD] = out["order_amount_clean"].where(success, 0.0)
    out[cfg.METRIC_FIELD] = out[cfg.METRIC_FIELD].fillna(0.0).astype(float)

    out["order_date_parsed"] = pd.to_datetime(
        out["order_date"],
        errors="coerce",
        format="mixed",
        utc=True,
    )
    return out


def build_working_dataset(
    frames: dict[str, pd.DataFrame] | None = None,
    verbose: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build a 300,000-row order-grain working dataset."""
    if frames is None:
        frames = load_frames()

    orders = prepare_orders(frames["orders"])
    customers, customer_report = canonicalize_customers(frames["customers"])
    products = prepare_products(frames["products"])
    click_features = reduce_clickstream(frames["clickstream"])
    support_features = reduce_support(frames["support"])

    rows_before = len(orders)
    start = time.perf_counter()

    working = (
        orders
        .merge(
            customers,
            on=cfg.CUSTOMER_KEY,
            how="left",
            validate="many_to_one",
        )
        .merge(
            products,
            on=cfg.PRODUCT_KEY,
            how="left",
            validate="many_to_one",
        )
        .merge(
            click_features,
            on=cfg.CUSTOMER_KEY,
            how="left",
            validate="many_to_one",
        )
        .merge(
            support_features,
            on=cfg.CUSTOMER_KEY,
            how="left",
            validate="many_to_one",
        )
    )

    join_seconds = time.perf_counter() - start

    count_cols = [
        "clickstream_events",
        "clickstream_sessions",
        "clickstream_devices",
        "support_tickets",
        "unresolved_tickets",
    ]
    for c in count_cols:
        if c in working:
            working[c] = working[c].fillna(0).astype("int64")

    rows_after = len(working)
    missing_state = int(working[cfg.PARTITION_KEY].isna().sum())
    missing_product = int(working["product_name"].isna().sum())

    report = {
        "rows_before": int(rows_before),
        "rows_after": int(rows_after),
        "delta": int(rows_after - rows_before),
        "columns_after": int(len(working.columns)),
        "join_seconds": round(join_seconds, 4),
        "join_path": (
            "orders |> canonical CRM |> product |> "
            "customer-level clickstream features |> customer-level support features"
        ),
        "customer_canonicalization": customer_report,
        "clickstream_feature_rows": int(len(click_features)),
        "support_feature_rows": int(len(support_features)),
        "missing_state_after_join": missing_state,
        "missing_product_after_join": missing_product,
    }

    assert rows_after == rows_before, (
        f"Join changed order grain: {rows_before:,} -> {rows_after:,}. "
        "A parent/reduced feature table is not unique on its join key."
    )
    assert missing_state == 0, (
        f"{missing_state} orders have no CRM state after the join."
    )
    assert missing_product == 0, (
        f"{missing_product} orders have no product after the join."
    )

    if verbose:
        cfg.banner("JOIN PATH RECONCILIATION")
        print(f"  join path              : {report['join_path']}")
        print(f"  rows before join       : {rows_before:,}")
        print(f"  rows after join        : {rows_after:,}")
        print(f"  difference             : {report['delta']}")
        print(f"  columns after          : {report['columns_after']}")
        print(f"  join time              : {report['join_seconds']:.4f} s")
        print(
            f"  CRM rows               : "
            f"{customer_report['raw_rows']:,} -> "
            f"{customer_report['canonical_rows']:,} "
            f"(removed {customer_report['duplicate_rows_removed']:,} duplicates)"
        )
        print(
            f"  clickstream features   : {len(click_features):,} customers"
        )
        print(
            f"  support features       : {len(support_features):,} customers"
        )

    return working, report


def main() -> int:
    cfg.banner("SESSION 1 — CommerceFlow LOAD AND JOIN")
    frames = load_frames()
    for name, df in frames.items():
        print(f"  loaded {name:<12} {len(df):>9,} rows")

    working, _ = build_working_dataset(frames)

    cfg.banner(f"PARTITION KEY PREVIEW — {cfg.PARTITION_KEY}")
    counts = working[cfg.PARTITION_KEY].value_counts()
    print(f"  distinct values : {counts.size}")
    print(
        f"  records/key     : min={counts.min():,} "
        f"median={int(counts.median()):,} max={counts.max():,}"
    )
    print(f"  skew ratio      : {counts.max() / counts.min():.2f} : 1")

    cfg.banner("MONETIZATION PREVIEW")
    print(f"  total orders            : {len(working):,}")
    print(
        f"  successful positive     : "
        f"{int(working['successful_order'].sum()):,}"
    )
    print(
        f"  revenue amount          : "
        f"{working[cfg.METRIC_FIELD].sum():,.2f}"
    )

    working.to_parquet(cfg.OUT_JOINED, index=False)
    print(f"\nWrote {cfg.OUT_JOINED} ({len(working):,} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
