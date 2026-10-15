"""
parallel_compute.py — PySpark local-mode implementation for CommerceFlow Session 1.

The Spark pipeline mirrors the pandas working-dataset logic:
1. canonicalize dirty CRM to one row per customer;
2. reduce clickstream and support to one row per customer;
3. left-join those features plus product data onto the 300,000 order rows;
4. repartition explicitly by ``state``;
5. compute order count, successful orders, revenue, and AOV;
6. validate against the pandas baseline.
"""
from __future__ import annotations

import json
import sys
import time

import pandas as pd

import config as cfg


def read_csv(spark, name: str):
    """Read a source CSV as strings so dirty values are cleaned explicitly."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(str(cfg.path_for(name)))
    )


def load_spark_frames(spark):
    return {
        name: read_csv(spark, name)
        for name in cfg.FILES
    }


def clean_numeric_spark(column_name: str):
    """Parse dirty numeric text with pandas-compatible missing handling.

    Important: Spark casting the literal string ``NaN`` to double produces a
    floating NaN, not SQL NULL. Spark also treats NaN as greater than ordinary
    numeric values, so a condition such as ``amount > 0`` can incorrectly mark
    a NaN amount as a successful positive-value order. Pandas ``to_numeric``
    treats the source ``NaN`` values as missing. Converting both NULL and NaN
    to SQL NULL here keeps the Spark and pandas cleaning rules identical.
    """
    from pyspark.sql import functions as F

    parsed = F.regexp_replace(
        F.trim(F.col(column_name).cast("string")),
        ",",
        "",
    ).cast("double")

    return F.when(
        parsed.isNull() | F.isnan(parsed),
        F.lit(None).cast("double"),
    ).otherwise(parsed)


def prepare_orders_spark(orders):
    from pyspark.sql import functions as F

    amount = clean_numeric_spark("order_amount")

    raw_status = F.lower(F.trim(F.col("status")))
    status_norm = (
        F.when(raw_status == "suc", F.lit("success"))
        .when(raw_status == "ref", F.lit("refunded"))
        .when(raw_status == "fail", F.lit("failed"))
        .otherwise(raw_status)
    )

    successful = (
        (status_norm == F.lit("success"))
        & amount.isNotNull()
        & (amount > F.lit(0.0))
    )

    return (
        orders
        .withColumn("order_amount_clean", amount)
        .withColumn("status_normalized", status_norm)
        .withColumn(
            "successful_order",
            F.when(successful, F.lit(1)).otherwise(F.lit(0)).cast("long"),
        )
        .withColumn(
            cfg.METRIC_FIELD,
            F.when(successful, amount).otherwise(F.lit(0.0)).cast("double"),
        )
    )


def canonicalize_customers_spark(customers):
    """Return one CRM row/customer after asserting state consistency."""
    from pyspark.sql import functions as F

    conflicts = (
        customers
        .groupBy(cfg.CUSTOMER_KEY)
        .agg(F.countDistinct(cfg.PARTITION_KEY).alias("_state_count"))
        .filter(F.col("_state_count") > 1)
        .count()
    )
    if conflicts:
        raise AssertionError(
            f"{conflicts} duplicated customer IDs disagree on "
            f"{cfg.PARTITION_KEY}; Spark canonicalization is unsafe."
        )

    selected = customers.select(
        cfg.CUSTOMER_KEY,
        "signup_date",
        "state",
        "country",
        "city",
        "source",
        "gender",
    )
    # Deterministic canonicalization: choose the lexicographically smallest
    # representation of the selected fields (row order in Spark is undefined).
    from pyspark.sql import Window
    stable_cols = [c for c in selected.columns if c != cfg.CUSTOMER_KEY]
    order_by = [F.col(c).asc_nulls_last() for c in stable_cols]
    window = Window.partitionBy(cfg.CUSTOMER_KEY).orderBy(*order_by)
    return (
        selected.withColumn("_canonical_rank", F.row_number().over(window))
        .filter(F.col("_canonical_rank") == 1)
        .drop("_canonical_rank")
    )


def prepare_products_spark(products):
    from pyspark.sql import functions as F

    return (
        products
        .withColumn(
            "product_price_clean",
            clean_numeric_spark("price"),
        )
        .withColumn(
            "category_normalized",
            F.lower(F.trim(F.col("category"))),
        )
        .select(
            cfg.PRODUCT_KEY,
            "product_name",
            "category",
            "category_normalized",
            "product_price_clean",
        )
    )


def reduce_clickstream_spark(clickstream):
    from pyspark.sql import functions as F

    return (
        clickstream
        .filter(F.col(cfg.CUSTOMER_KEY).isNotNull())
        .groupBy(cfg.CUSTOMER_KEY)
        .agg(
            F.count(cfg.CLICKSTREAM_KEY).alias("clickstream_events"),
            F.countDistinct("session_id").alias("clickstream_sessions"),
            F.countDistinct("device_id").alias("clickstream_devices"),
        )
    )


def reduce_support_spark(support):
    from pyspark.sql import functions as F

    resolution = clean_numeric_spark("resolution_time_hours")

    return (
        support
        .withColumn("_resolution_hours", resolution)
        .withColumn(
            "_unresolved",
            F.when(F.col("ticket_resolved").isNull(), F.lit(1))
            .otherwise(F.lit(0)),
        )
        .groupBy(cfg.CUSTOMER_KEY)
        .agg(
            F.count(cfg.SUPPORT_KEY).alias("support_tickets"),
            F.avg("_resolution_hours").alias("avg_resolution_time_hours"),
            F.sum("_unresolved").alias("unresolved_tickets"),
        )
    )


def build_joined(spark, verbose: bool = True):
    """Build the Spark order-grain working DataFrame with broadcast joins."""
    from pyspark.sql import functions as F

    frames = load_spark_frames(spark)
    orders = prepare_orders_spark(frames["orders"])
    customers = canonicalize_customers_spark(frames["customers"])
    products = prepare_products_spark(frames["products"])
    click_features = reduce_clickstream_spark(frames["clickstream"])
    support_features = reduce_support_spark(frames["support"])

    rows_before = orders.count()
    input_partitions = orders.rdd.getNumPartitions()

    joined = (
        orders
        .join(
            F.broadcast(customers),
            on=cfg.CUSTOMER_KEY,
            how="left",
        )
        .join(
            F.broadcast(products),
            on=cfg.PRODUCT_KEY,
            how="left",
        )
        .join(
            F.broadcast(click_features),
            on=cfg.CUSTOMER_KEY,
            how="left",
        )
        .join(
            F.broadcast(support_features),
            on=cfg.CUSTOMER_KEY,
            how="left",
        )
        .fillna(
            0,
            subset=[
                "clickstream_events",
                "clickstream_sessions",
                "clickstream_devices",
                "support_tickets",
                "unresolved_tickets",
            ],
        )
    )

    joined.cache()
    rows_after = joined.count()

    missing_state = joined.filter(F.col(cfg.PARTITION_KEY).isNull()).count()
    missing_product = joined.filter(F.col("product_name").isNull()).count()

    plan = joined._jdf.queryExecution().executedPlan().toString()
    broadcast_nodes = plan.count("BroadcastHashJoin")
    sortmerge_nodes = plan.count("SortMergeJoin")

    report = {
        "rows_before": int(rows_before),
        "rows_after": int(rows_after),
        "delta": int(rows_after - rows_before),
        "input_partitions": int(input_partitions),
        "broadcast_hash_joins": int(broadcast_nodes),
        "sort_merge_joins": int(sortmerge_nodes),
        "missing_state_after_join": int(missing_state),
        "missing_product_after_join": int(missing_product),
    }

    if verbose:
        cfg.banner("SPARK JOIN")
        print(f"  rows before join       : {rows_before:,}")
        print(f"  rows after join        : {rows_after:,}")
        print(f"  difference             : {report['delta']}")
        print(f"  input partitions       : {input_partitions}")
        print(f"  BroadcastHashJoin nodes: {broadcast_nodes}")
        print(f"  SortMergeJoin nodes    : {sortmerge_nodes}")
        if sortmerge_nodes == 0:
            print("  -> no SortMergeJoin detected; reduced/small tables broadcast")
        else:
            print("  -> shuffle join detected; inspect joined.explain('formatted')")

    assert rows_after == rows_before, "Spark join changed the 300,000-order grain"
    assert missing_state == 0, "Spark join left orders without state"
    assert missing_product == 0, "Spark join left orders without product"

    return joined, report


def compute_parallel(joined, partitions: int):
    """Repartition by state with an explicit bound, then aggregate."""
    from pyspark.sql import functions as F

    partitioned = joined.repartition(partitions, cfg.PARTITION_KEY)

    totals = (
        partitioned
        .groupBy(cfg.PARTITION_KEY)
        .agg(
            F.count(cfg.ORDER_KEY).cast("long").alias("order_count"),
            F.sum("successful_order").cast("long").alias("successful_orders"),
            F.sum(cfg.METRIC_FIELD).cast("double").alias("revenue_total"),
        )
    )

    result = totals.withColumn(
        "aov",
        F.when(
            F.col("successful_orders") > 0,
            F.col("revenue_total") / F.col("successful_orders"),
        ).otherwise(F.lit(0.0)),
    )

    return partitioned, result.select(
        cfg.PARTITION_KEY,
        "order_count",
        "successful_orders",
        "revenue_total",
        "aov",
    )


def validate(
    parallel_pd: pd.DataFrame,
    baseline_pd: pd.DataFrame,
    verbose: bool = True,
) -> dict:
    """Compare group counts, integer metrics, and float metrics."""
    parallel_pd = (
        parallel_pd
        .sort_values(cfg.PARTITION_KEY)
        .reset_index(drop=True)
    )
    baseline_pd = (
        baseline_pd
        .sort_values(cfg.PARTITION_KEY)
        .reset_index(drop=True)
    )

    group_match = len(parallel_pd) == len(baseline_pd)
    merged = parallel_pd.merge(
        baseline_pd,
        on=cfg.PARTITION_KEY,
        suffixes=("_par", "_base"),
        how="outer",
        indicator=True,
    )
    key_match = bool((merged["_merge"] == "both").all())

    # Outer joins yield NaN for absent keys. Never coerce a missing metric to
    # int: diagnose the mismatch as a failed validation instead of crashing.
    complete = merged["_merge"].eq("both")
    order_series = (
        merged["order_count_par"] - merged["order_count_base"]
    ).abs()
    successful_series = (
        merged["successful_orders_par"] - merged["successful_orders_base"]
    ).abs()
    revenue_series = (
        merged["revenue_total_par"] - merged["revenue_total_base"]
    ).abs()
    aov_series = (merged["aov_par"] - merged["aov_base"]).abs()

    order_count_diff = int(order_series.max()) if complete.all() and len(merged) else None
    successful_diff = int(successful_series.max()) if complete.all() and len(merged) else None
    revenue_diff = float(revenue_series.max()) if complete.all() and len(merged) else None
    aov_diff = float(aov_series.max()) if complete.all() and len(merged) else None

    import math
    finite_metrics = (
        revenue_diff is not None and aov_diff is not None
        and math.isfinite(revenue_diff) and math.isfinite(aov_diff)
    )
    passed = (
        group_match
        and key_match
        and order_count_diff == 0
        and successful_diff == 0
        and finite_metrics
        and revenue_diff <= cfg.TOLERANCE
        and aov_diff <= cfg.TOLERANCE
    )

    report = {
        "parallel_groups": int(len(parallel_pd)),
        "baseline_groups": int(len(baseline_pd)),
        "group_count_match": bool(group_match),
        "group_keys_match": bool(key_match),
        "max_order_count_difference": order_count_diff,
        "max_successful_orders_difference": successful_diff,
        "max_revenue_total_difference": revenue_diff,
        "max_aov_difference": aov_diff,
        "tolerance": cfg.TOLERANCE,
        "passed": bool(passed),
    }

    if verbose:
        cfg.banner("CORRECTNESS VALIDATION")
        print(f"  parallel groups               : {report['parallel_groups']}")
        print(f"  baseline groups               : {report['baseline_groups']}")
        print(f"  max order_count difference    : {order_count_diff}")
        print(f"  max successful difference     : {successful_diff}")
        print(f"  max revenue_total difference  : {revenue_diff}")
        print(f"  max AOV difference            : {aov_diff}")
        print(f"  tolerance                     : {cfg.TOLERANCE:.0e}")
        print(f"\n  {'PASSED' if passed else 'FAILED'}")

    if not passed:
        debug = merged.copy()
        debug["order_count_diff"] = order_series
        debug["successful_diff"] = successful_series
        debug["revenue_diff"] = revenue_series
        debug["aov_diff"] = aov_series
        mismatch = debug[
            (debug["_merge"] != "both")
            | (debug["order_count_diff"].fillna(0) != 0)
            | (debug["successful_diff"].fillna(0) != 0)
            | (debug["revenue_diff"].fillna(float("inf")) > cfg.TOLERANCE)
            | (debug["aov_diff"].fillna(float("inf")) > cfg.TOLERANCE)
        ]
        if len(mismatch):
            print("\nTop mismatching states (diagnostic):")
            cols = [
                cfg.PARTITION_KEY,
                "order_count_diff",
                "successful_diff",
                "revenue_diff",
                "aov_diff",
            ]
            print(mismatch[cols].head(10).to_string(index=False))

        raise AssertionError(
            "Parallel output does not match the pandas baseline. "
            "Spark and pandas must use identical missing-value, status, "
            "amount-cleaning, and aggregation rules."
        )

    return report


def main() -> int:
    cfg.banner("SESSION 1 — CommerceFlow PARALLEL COMPUTE")
    spark = cfg.build_spark()

    try:
        joined, join_report = build_joined(spark)

        cfg.banner(
            f"PARALLEL AGGREGATION — {cfg.CHOSEN_PARTITIONS} PARTITIONS"
        )
        start = time.perf_counter()
        partitioned, result = compute_parallel(
            joined,
            cfg.CHOSEN_PARTITIONS,
        )
        result.cache()
        groups = result.count()  # action: forces the timed aggregation
        elapsed = time.perf_counter() - start

        print(
            f"  configured partitions : "
            f"{partitioned.rdd.getNumPartitions()}"
        )
        print(f"  result groups          : {groups}")
        print(f"  execution time         : {elapsed:.4f} s")
        result.orderBy("revenue_total", ascending=False).show(
            10,
            truncate=False,
        )

        parallel_pd = result.toPandas()
        # Compute the reference from the *current* CSVs. A saved baseline may
        # belong to a previous dataset version and must not be trusted blindly.
        print("\nRecomputing pandas baseline from current source datasets...")
        from sequential_baseline import run_baseline
        baseline_pd, _ = run_baseline(verbose=False)
        baseline_pd.sort_values(cfg.PARTITION_KEY).to_csv(
            cfg.OUT_BASELINE, index=False
        )

        validation = validate(parallel_pd, baseline_pd)

        final = (
            parallel_pd
            .sort_values(cfg.PARTITION_KEY)
            .reset_index(drop=True)
        )
        final.to_parquet(cfg.OUT_FINAL, index=False)
        print(f"\nWrote {cfg.OUT_FINAL} ({len(final)} rows)")

        cfg.OUT_VALIDATION.write_text(
            json.dumps(
                {
                    "join": join_report,
                    "aggregation": {
                        "partitions": cfg.CHOSEN_PARTITIONS,
                        "groups": int(groups),
                        "seconds": round(elapsed, 4),
                    },
                    "validation": validation,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"Wrote {cfg.OUT_VALIDATION}")

        result.unpersist()
        joined.unpersist()
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
