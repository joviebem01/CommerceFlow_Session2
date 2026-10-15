"""
benchmark.py — controlled comparison of pandas vs bounded Spark parallelism.

Everything is held constant except the Spark partition count (2, 4, 8).
Each condition runs repeatedly and the median is reported.
"""
from __future__ import annotations

import csv
import statistics
import sys
import time

import config as cfg
from load_and_join import build_working_dataset
from parallel_compute import build_joined, compute_parallel
from sequential_baseline import run_baseline


def bench_one(joined, partitions: int, repeats: int) -> dict:
    times: list[float] = []
    groups = None

    for _ in range(repeats):
        start = time.perf_counter()
        _, result = compute_parallel(joined, partitions)
        groups = result.count()  # action: forces execution
        times.append(time.perf_counter() - start)

    return {
        "partitions": partitions,
        "runs": [round(t, 6) for t in times],
        "median": round(statistics.median(times), 6),
        "min": round(min(times), 6),
        "max": round(max(times), 6),
        "groups": int(groups),
    }


def main() -> int:
    cfg.banner("SESSION 1 — CommerceFlow BENCHMARK")

    working, join_report = build_working_dataset(verbose=False)
    _, baseline_report = run_baseline(working, verbose=False)

    print(f"  pandas join time      : {join_report['join_seconds']:.4f} s")
    print(
        f"  baseline median       : "
        f"{baseline_report['median_seconds']:.6f} s "
        f"over {baseline_report['repeats']} runs"
    )
    print(f"  baseline groups       : {baseline_report['groups']}")

    spark = cfg.build_spark()
    rows: list[dict] = []

    try:
        joined, spark_join = build_joined(spark, verbose=False)
        print(f"  Spark joined rows     : {joined.count():,}")
        print(
            f"  BroadcastHashJoin     : "
            f"{spark_join['broadcast_hash_joins']}"
        )
        print(f"  SortMergeJoin         : {spark_join['sort_merge_joins']}")

        cfg.banner("BOUNDED PARALLELISM CONDITIONS")
        results = []
        for partitions in cfg.PARTITION_SETTINGS:
            r = bench_one(
                joined,
                partitions,
                cfg.BENCHMARK_REPEATS,
            )
            results.append(r)
            runs = ", ".join(f"{t:.4f}" for t in r["runs"])
            print(
                f"  partitions={partitions:>2} | "
                f"median={r['median']:.4f} s | "
                f"groups={r['groups']} | runs=[{runs}]"
            )

        group_counts = {r["groups"] for r in results}
        group_counts.add(baseline_report["groups"])
        assert len(group_counts) == 1, (
            f"Conditions disagree on group count: {group_counts}. "
            "The benchmark is invalid if conditions compute different outputs."
        )

        base_median = baseline_report["median_seconds"]
        rows.append({
            "run": "Sequential baseline",
            "parallelism_partitions": "1 / non-parallel",
            "execution_time_s": f"{base_median:.6f}",
            "groups": baseline_report["groups"],
            "correct": "Yes",
            "speedup_vs_baseline": "1.0000",
            "observation": "pandas, in-process, no JVM scheduling",
        })

        best = min(results, key=lambda r: r["median"])
        for r in results:
            speedup = (
                base_median / r["median"]
                if r["median"] > 0 else float("inf")
            )
            note = (
                "fastest parallel setting"
                if r is best
                else "slower than the best parallel setting"
            )
            rows.append({
                "run": f"Parallel ({r['partitions']} partitions)",
                "parallelism_partitions": r["partitions"],
                "execution_time_s": f"{r['median']:.6f}",
                "groups": r["groups"],
                "correct": "Yes",
                "speedup_vs_baseline": f"{speedup:.4f}",
                "observation": note,
            })

        cfg.banner("INTERPRETATION")
        print(
            f"  best parallel setting : {best['partitions']} partitions "
            f"at {best['median']:.4f} s"
        )
        ratio = best["median"] / base_median
        if ratio > 1:
            print(
                f"  Spark is {ratio:.1f}x slower than the pandas baseline "
                f"for this in-memory workload."
            )
            print(
                "  At this size, JVM/task scheduling and Python-JVM "
                "coordination may dominate the aggregation."
            )
        else:
            print(f"  Spark is {1 / ratio:.2f}x faster than the baseline.")

        if spark_join["sort_merge_joins"] == 0:
            print(
                "  No SortMergeJoin was detected; partition-count "
                "differences mainly reflect scheduling/partitioning overhead."
            )

        joined.unpersist()
    finally:
        spark.stop()

    fields = [
        "run",
        "parallelism_partitions",
        "execution_time_s",
        "groups",
        "correct",
        "speedup_vs_baseline",
        "observation",
    ]
    with open(cfg.OUT_BENCHMARK, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {cfg.OUT_BENCHMARK}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
