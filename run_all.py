"""Run the CommerceFlow Session 1 pipeline in the required order.

Examples
--------
Full pipeline (requires pandas, pyarrow, PySpark/JDK, and Graphviz):
    python run_all.py

Pandas-only verification plus diagrams:
    python run_all.py --skip-spark

Skip diagram rendering when Graphviz ``dot`` is unavailable:
    python run_all.py --skip-diagrams
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(script: str) -> None:
    print("\n" + "#" * 78)
    print(f"RUNNING: {script}")
    print("#" * 78)
    subprocess.run(
        [sys.executable, str(ROOT / script)],
        cwd=ROOT,
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the CommerceFlow MIT 261 Session 1 pipeline."
    )
    parser.add_argument(
        "--skip-spark",
        action="store_true",
        help="Skip parallel_compute.py, benchmark.py, and partition_analysis.py.",
    )
    parser.add_argument(
        "--skip-diagrams",
        action="store_true",
        help="Skip render_diagrams.py (useful if Graphviz dot is not installed).",
    )
    args = parser.parse_args()

    core = [
        "profile_files.py",
        "load_and_join.py",
        "partition_strategy.py",
        "sequential_baseline.py",
    ]
    for script in core:
        run(script)

    if not args.skip_spark:
        for script in (
            "parallel_compute.py",
            "benchmark.py",
            "partition_analysis.py",
        ):
            run(script)

    if not args.skip_diagrams:
        run("render_diagrams.py")

    print("\nPipeline completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
