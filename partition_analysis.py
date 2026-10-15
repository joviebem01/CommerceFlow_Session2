"""
partition_analysis.py — compare key-level skew with physical Spark-partition skew.
"""
from __future__ import annotations

import csv
import math
import sys

import config as cfg
from load_and_join import build_working_dataset
from parallel_compute import build_joined


def key_level(working) -> dict:
    counts = working[cfg.PARTITION_KEY].value_counts()
    even = len(working) / len(counts)
    return {
        "distinct_keys": int(counts.size),
        "min": int(counts.min()),
        "median": int(counts.median()),
        "max": int(counts.max()),
        "even_share": round(even, 1),
        "skew_ratio": round(float(counts.max() / counts.min()), 4),
        "heaviest": counts.head(5).to_dict(),
        "lightest": counts.tail(5).to_dict(),
        "counts": counts,
    }


def partition_level(joined, partitions: int) -> dict:
    """Count rows in each physical Spark partition after hash repartition."""
    from pyspark.sql import functions as F

    rows = (
        joined
        .repartition(partitions, cfg.PARTITION_KEY)
        .select(F.spark_partition_id().alias("_partition_id"))
        .groupBy("_partition_id")
        .count()
        .collect()
    )
    by_id = {int(r["_partition_id"]): int(r["count"]) for r in rows}
    sizes = [by_id.get(i, 0) for i in range(partitions)]

    total = sum(sizes)
    even = total / partitions
    positive = [s for s in sizes if s > 0]
    minimum = min(sizes)
    maximum = max(sizes)
    skew = (
        maximum / min(positive)
        if positive and minimum > 0
        else math.inf
    )

    return {
        "partitions": partitions,
        "sizes": sizes,
        "min": minimum,
        "max": maximum,
        "even_share": round(even, 1),
        "skew_ratio": round(skew, 4) if math.isfinite(skew) else "inf",
        "worst_vs_even": round(maximum / even, 4) if even else 0.0,
    }


def main() -> int:
    cfg.banner("SESSION 1 — CommerceFlow PARTITION ANALYSIS")

    working, _ = build_working_dataset(verbose=False)
    kl = key_level(working)

    cfg.banner(f"KEY LEVEL — {cfg.PARTITION_KEY}")
    print(f"  distinct keys : {kl['distinct_keys']}")
    print(
        f"  records/key   : min={kl['min']:,} "
        f"median={kl['median']:,} max={kl['max']:,}"
    )
    print(f"  skew ratio    : {kl['skew_ratio']:.2f} : 1")
    print("\n  heaviest 5:")
    for key, count in kl["heaviest"].items():
        print(f"    {str(key):<20} {count:>7,}")
    print("  lightest 5:")
    for key, count in kl["lightest"].items():
        print(f"    {str(key):<20} {count:>7,}")

    rows: list[dict] = []
    for key, count in kl["counts"].items():
        rows.append({
            "level": "partition_key",
            "setting": cfg.PARTITION_KEY,
            "identifier": key,
            "record_count": int(count),
            "even_share": kl["even_share"],
            "vs_even": round(count / kl["even_share"], 4),
        })

    spark = cfg.build_spark()
    try:
        joined, _ = build_joined(spark, verbose=False)

        cfg.banner("PHYSICAL SPARK PARTITIONS")
        print(
            f"  {'n':>3} {'even':>10} {'min':>10} "
            f"{'max':>10} {'ratio':>9} {'worst/even':>12}"
        )
        print("  " + "-" * 61)

        physical_reports = []
        for partitions in cfg.PARTITION_SETTINGS:
            pl = partition_level(joined, partitions)
            physical_reports.append(pl)
            ratio = pl["skew_ratio"]
            ratio_text = f"{ratio:.2f}" if isinstance(ratio, float) else ratio
            print(
                f"  {partitions:>3} {pl['even_share']:>10,.1f} "
                f"{pl['min']:>10,} {pl['max']:>10,} "
                f"{ratio_text:>9} {pl['worst_vs_even']:>12.2f}"
            )

            for idx, size in enumerate(pl["sizes"]):
                rows.append({
                    "level": "spark_partition",
                    "setting": partitions,
                    "identifier": f"partition_{idx}",
                    "record_count": size,
                    "even_share": pl["even_share"],
                    "vs_even": (
                        round(size / pl["even_share"], 4)
                        if pl["even_share"] else 0.0
                    ),
                })

        cfg.banner("INTERPRETATION")
        print(
            f"  Key-level state skew is {kl['skew_ratio']:.2f}:1. "
            "Physical partition skew can differ because Spark hashes "
            "multiple states into each partition."
        )
        worst = max(
            physical_reports,
            key=lambda r: r["worst_vs_even"],
        )
        print(
            f"  Largest observed straggler load: n={worst['partitions']}, "
            f"{worst['max']:,} rows = "
            f"{worst['worst_vs_even']:.2f}x its even share."
        )

        joined.unpersist()
    finally:
        spark.stop()

    fields = [
        "level",
        "setting",
        "identifier",
        "record_count",
        "even_share",
        "vs_even",
    ]
    with open(cfg.OUT_PARTITIONS, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {cfg.OUT_PARTITIONS} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
