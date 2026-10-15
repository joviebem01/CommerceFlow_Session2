"""
profile_files.py — Session 1 profiling, integrity, and eligibility for CommerceFlow.

Profiles all five CSVs, detects candidate keys, measures nulls/cardinality,
verifies cross-file references, and records the CRM duplicate-key condition that
must be handled before joining.
"""
from __future__ import annotations

import json
import sys
from typing import Any

import pandas as pd

import config as cfg


def profile_one(name: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = cfg.path_for(name)
    df = pd.read_csv(path, low_memory=False)

    candidate_pks = [
        c for c in df.columns
        if df[c].notna().all() and df[c].is_unique
    ]
    constant_cols = [
        c for c in df.columns
        if df[c].nunique(dropna=True) <= 1
    ]

    primary_key = cfg.FILES[name]["pk"]
    duplicate_pk_rows = (
        int(df.duplicated(primary_key).sum())
        if primary_key in df.columns else None
    )

    prof = {
        "file": path.name,
        "role": cfg.FILES[name]["role"],
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "column_names": df.columns.tolist(),
        "size_mb": round(path.stat().st_size / (1024 * 1024), 2),
        "declared_primary_key": primary_key,
        "candidate_primary_keys": candidate_pks,
        "duplicate_primary_key_rows": duplicate_pk_rows,
        "constant_columns": constant_cols,
        "null_counts": {c: int(df[c].isna().sum()) for c in df.columns},
        "distinct_counts": {
            c: int(df[c].nunique(dropna=True)) for c in df.columns
        },
    }
    return df, prof


def _fk_result(child: pd.Series, parent_values: set) -> dict[str, Any]:
    nonnull = child.dropna()
    unresolved = ~nonnull.isin(parent_values)
    return {
        "rows_checked": int(len(nonnull)),
        "orphans": int(unresolved.sum()),
        "resolved": bool((~unresolved).all()),
    }


def check_integrity(frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    customers = frames["customers"]
    orders = frames["orders"]
    products = frames["products"]
    clickstream = frames["clickstream"]
    support = frames["support"]

    customer_ids = set(customers[cfg.CUSTOMER_KEY])
    product_ids = set(products[cfg.PRODUCT_KEY])

    checks = {
        "orders.customer_id -> customers.customer_id":
            _fk_result(orders[cfg.CUSTOMER_KEY], customer_ids),
        "orders.product_id -> products.product_id":
            _fk_result(orders[cfg.PRODUCT_KEY], product_ids),
        "support.customer_id -> customers.customer_id":
            _fk_result(support[cfg.CUSTOMER_KEY], customer_ids),
        "clickstream.customer_id(non-null) -> customers.customer_id":
            _fk_result(clickstream[cfg.CUSTOMER_KEY], customer_ids),
    }

    dup_counts = customers[cfg.CUSTOMER_KEY].value_counts()
    duplicate_ids = dup_counts[dup_counts > 1].index
    duplicate_rows = int(customers.duplicated(cfg.CUSTOMER_KEY).sum())

    # The partition key must be stable across duplicate CRM copies.
    # If this is not true, choosing one canonical customer row would be unsafe.
    duplicate_subset = customers[
        customers[cfg.CUSTOMER_KEY].isin(duplicate_ids)
    ]
    state_conflicts = 0
    if len(duplicate_subset):
        state_conflicts = int(
            duplicate_subset.groupby(cfg.CUSTOMER_KEY)[cfg.PARTITION_KEY]
            .nunique(dropna=False)
            .gt(1)
            .sum()
        )

    return {
        "foreign_keys": checks,
        "crm_duplicate_customer_ids": int(len(duplicate_ids)),
        "crm_duplicate_rows_to_remove": duplicate_rows,
        "crm_max_rows_per_customer": int(dup_counts.max()),
        "crm_partition_key_conflicts_across_duplicates": state_conflicts,
        "clickstream_anonymous_rows": int(
            clickstream[cfg.CUSTOMER_KEY].isna().sum()
        ),
    }


def check_eligibility(
    frames: dict[str, pd.DataFrame],
    profiles: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    orders = frames["orders"]
    clickstream = frames["clickstream"]
    support = frames["support"]

    qualifying = [
        n for n, p in profiles.items()
        if p["role"] in ("Event", "Entity")
    ]

    relationships = []
    for parent, child, key in [
        ("customers", "orders", cfg.CUSTOMER_KEY),
        ("customers", "clickstream", cfg.CUSTOMER_KEY),
        ("customers", "support", cfg.CUSTOMER_KEY),
        ("products", "orders", cfg.PRODUCT_KEY),
    ]:
        child_df = frames[child]
        series = child_df[key].dropna()
        if not series.is_unique:
            counts = series.value_counts()
            relationships.append({
                "parent": parent,
                "child": child,
                "key": key,
                "children_min": int(counts.min()),
                "children_median": int(counts.median()),
                "children_max": int(counts.max()),
            })

    timestamp_candidates = {
        "orders.order_date": orders["order_date"],
        "clickstream.timestamp": clickstream["timestamp"],
        "support.ticket_created": support["ticket_created"],
    }
    timestamp_report = {}
    best_parse_rate = 0.0
    for label, series in timestamp_candidates.items():
        parsed = pd.to_datetime(series, errors="coerce", format="mixed", utc=True)
        parse_rate = float(parsed.notna().mean())
        best_parse_rate = max(best_parse_rate, parse_rate)
        valid = parsed.dropna()
        timestamp_report[label] = {
            "parse_rate": round(parse_rate, 4),
            "valid_rows": int(parsed.notna().sum()),
            "min": str(valid.min()) if len(valid) else None,
            "max": str(valid.max()) if len(valid) else None,
        }

    event_rows = sum(
        p["rows"] for p in profiles.values() if p["role"] == "Event"
    )

    return {
        "condition_1_three_related_files": {
            "met": len(qualifying) >= 3,
            "qualifying_files": qualifying,
        },
        "condition_2_one_to_many": {
            "met": len(relationships) >= 1,
            "associations": relationships,
        },
        "condition_3_timestamp": {
            "met": best_parse_rate > 0.5,
            "candidates": timestamp_report,
        },
        "condition_4_volume": {
            "met": event_rows >= 50_000,
            "event_rows": int(event_rows),
        },
    }


def main() -> int:
    cfg.banner("SESSION 1 — CommerceFlow FILE PROFILING")

    frames: dict[str, pd.DataFrame] = {}
    profiles: dict[str, dict[str, Any]] = {}

    for name in cfg.FILES:
        df, prof = profile_one(name)
        frames[name] = df
        profiles[name] = prof

        print(
            f"\nFILE: {prof['file']}\n"
            f"  role={prof['role']}  rows={prof['rows']:,}  "
            f"cols={prof['columns']}  size={prof['size_mb']} MB"
        )
        print(f"  columns: {', '.join(prof['column_names'])}")
        print(f"  candidate primary keys: {prof['candidate_primary_keys']}")
        if prof["duplicate_primary_key_rows"]:
            print(
                f"  WARNING: {prof['duplicate_primary_key_rows']:,} duplicate "
                f"row(s) on declared key {prof['declared_primary_key']}"
            )
        null_total = sum(prof["null_counts"].values())
        print(f"  total null cells: {null_total:,}")

    cfg.banner("REFERENTIAL INTEGRITY")
    integrity = check_integrity(frames)
    for label, result in integrity["foreign_keys"].items():
        print(
            f"  {'PASS' if result['resolved'] else 'FAIL'}  {label}  "
            f"orphans={result['orphans']:,}"
        )

    print(
        "\nCRM duplicate handling:\n"
        f"  duplicate customer IDs : "
        f"{integrity['crm_duplicate_customer_ids']:,}\n"
        f"  duplicate rows to drop : "
        f"{integrity['crm_duplicate_rows_to_remove']:,}\n"
        f"  max copies/customer    : "
        f"{integrity['crm_max_rows_per_customer']}\n"
        f"  state conflicts        : "
        f"{integrity['crm_partition_key_conflicts_across_duplicates']}"
    )
    print(
        f"  anonymous clickstream rows: "
        f"{integrity['clickstream_anonymous_rows']:,}"
    )

    cfg.banner("DATASET ELIGIBILITY")
    eligibility = check_eligibility(frames, profiles)
    for key, result in eligibility.items():
        print(f"  {'MET' if result['met'] else 'NOT MET'}  {key}")

    report = {
        "profiles": profiles,
        "integrity": integrity,
        "eligibility": eligibility,
    }
    cfg.OUT_PROFILE.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nWrote {cfg.OUT_PROFILE}")

    all_met = all(v["met"] for v in eligibility.values())
    all_fk = all(v["resolved"] for v in integrity["foreign_keys"].values())
    safe_dedup = (
        integrity["crm_partition_key_conflicts_across_duplicates"] == 0
    )

    if not (all_met and all_fk and safe_dedup):
        print(
            "\nDATASET GATE FAILED. Fix unresolved keys, eligibility, or "
            "conflicting duplicate CRM states before proceeding."
        )
        return 1

    print("\nAll Session 1 gates passed. Proceed to load_and_join.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
