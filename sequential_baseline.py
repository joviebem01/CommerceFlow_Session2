"""
sequential_baseline.py — pandas reference result for the CommerceFlow workload.

The same order-grain working dataset and the same monetization definition are
used by the Spark implementation. The baseline is both the performance control
and the correctness reference.
"""
from __future__ import annotations

import statistics
import sys
import time
from typing import Any

import numpy as np
import pandas as pd

import config as cfg
from load_and_join import build_working_dataset


def compute(working: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        working.groupby(cfg.PARTITION_KEY, as_index=False)
        .agg(
            order_count=(cfg.ORDER_KEY, "count"),
            successful_orders=("successful_order", "sum"),
            revenue_total=(cfg.METRIC_FIELD, "sum"),
        )
    )
    grouped["successful_orders"] = grouped["successful_orders"].astype("int64")
    grouped["order_count"] = grouped["order_count"].astype("int64")
    grouped["aov"] = np.where(
        grouped["successful_orders"] > 0,
        grouped["revenue_total"] / grouped["successful_orders"],
        0.0,
    )
    return grouped[
        [
            cfg.PARTITION_KEY,
            "order_count",
            "successful_orders",
            "revenue_total",
            "aov",
        ]
    ]


def run_baseline(
    working: pd.DataFrame | None = None,
    repeats: int = cfg.BASELINE_REPEATS,
    verbose: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if working is None:
        working, _ = build_working_dataset(verbose=False)

    times: list[float] = []
    result: pd.DataFrame | None = None

    for _ in range(repeats):
        start = time.perf_counter()
        result = compute(working)
        times.append(time.perf_counter() - start)

    assert result is not None
    report = {
        "runs": [round(t, 6) for t in times],
        "median_seconds": round(statistics.median(times), 6),
        "mean_seconds": round(statistics.fmean(times), 6),
        "groups": int(len(result)),
        "repeats": repeats,
    }

    if verbose:
        cfg.banner("SEQUENTIAL BASELINE")
        for i, t in enumerate(times, 1):
            print(f"  run {i}: {t:.6f} s")
        print(f"\n  median : {report['median_seconds']:.6f} s")
        print(f"  mean   : {report['mean_seconds']:.6f} s")
        print(f"  groups : {report['groups']}")

    return result, report


def main() -> int:
    cfg.banner("SESSION 1 — CommerceFlow SEQUENTIAL BASELINE")
    working, _ = build_working_dataset()
    result, _ = run_baseline(working)

    ranked = result.sort_values("revenue_total", ascending=False)
    print("\nTop 10 states by revenue:")
    print(ranked.head(10).to_string(index=False))

    print(
        f"\nTotal monetized revenue: "
        f"{result['revenue_total'].sum():,.2f}"
    )
    print(
        f"Successful positive-value orders: "
        f"{int(result['successful_orders'].sum()):,}"
    )

    result.sort_values(cfg.PARTITION_KEY).to_csv(
        cfg.OUT_BASELINE,
        index=False,
    )
    print(f"\nWrote {cfg.OUT_BASELINE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
