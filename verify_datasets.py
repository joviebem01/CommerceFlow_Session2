"""Verify the packaged datasets and pandas computation (no Spark needed)."""
from __future__ import annotations
import sys
from load_and_join import load_frames, build_working_dataset
from sequential_baseline import compute
import config as cfg

def main():
    frames = load_frames()
    for name, frame in frames.items():
        print(f"{name:12} {len(frame):>9,} rows | {len(frame.columns):2} columns")
    working, report = build_working_dataset(frames)
    result = compute(working)
    assert len(working) == len(frames['orders'])
    assert result['order_count'].sum() == len(frames['orders'])
    assert result['successful_orders'].sum() == working['successful_order'].sum()
    assert abs(result['revenue_total'].sum() - working[cfg.METRIC_FIELD].sum()) < 0.01
    assert report['missing_state_after_join'] == 0
    assert report['missing_product_after_join'] == 0
    print(f"PASS: {len(result)} states; {len(working):,} orders; "
          f"{int(result['successful_orders'].sum()):,} successful orders; "
          f"{result['revenue_total'].sum():,.2f} revenue")
    return 0

if __name__ == '__main__':
    sys.exit(main())
