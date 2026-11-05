#!/usr/bin/env python3
"""
Trình điều phối chạy các kịch bản thực nghiệm Chương 11 (Kịch bản 1 -> Kịch bản 7).
Cho phép chạy từng kịch bản hoặc chạy toàn bộ chuỗi thực nghiệm.
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scenarios.kb1_basic_crud import run_kb1
from scenarios.kb2_replication_sync import run_kb2
from scenarios.kb3_node_failure import run_kb3
from scenarios.kb4_failover import run_kb4
from scenarios.kb5_recovery import run_kb5
from scenarios.kb6_consistency import run_kb6
from scenarios.kb7_performance import run_kb7


def main():
    parser = argparse.ArgumentParser(description="Chạy bộ kịch bản thử nghiệm Cluster (Chương 11)")
    parser.add_argument(
        "--scenario",
        choices=["all", "kb1", "kb2", "kb3", "kb4", "kb5", "kb6", "kb7"],
        default="all",
        help="Chọn kịch bản cần chạy (mặc định: all)",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Chạy chế độ kiểm thử nhanh để xác minh toàn bộ luồng kịch bản",
    )
    args = parser.parse_args()

    if args.scenario in ("all", "kb1"):
        run_kb1(iterations=50 if args.quick else 100)

    if args.scenario in ("all", "kb2"):
        run_kb2(
            samples=50 if args.quick else 200,
            batch_sizes=[1000, 5000] if args.quick else [1000, 10000, 50000],
        )

    if args.scenario in ("all", "kb3"):
        run_kb3(
            mode="both",
            warmup_sec=5 if args.quick else 15,
            degraded_sec=10 if args.quick else 30,
        )

    if args.scenario in ("all", "kb4"):
        run_kb4(
            pre_sec=5 if args.quick else 10,
            post_sec=8 if args.quick else 15,
        )

    if args.scenario in ("all", "kb5"):
        run_kb5(missed_ops=2000 if args.quick else 10000)

    if args.scenario in ("all", "kb6"):
        run_kb6(iterations=200 if args.quick else 1000)

    if args.scenario in ("all", "kb7"):
        run_kb7(
            profiles=["P1", "P2", "P3"],
            concurrencies=[1, 5, 10],
            workload="mixed_80_20",
            scale=1000,
            warmup_sec=3 if args.quick else 30,
            measure_sec=8 if args.quick else 120,
        )


if __name__ == "__main__":
    main()
