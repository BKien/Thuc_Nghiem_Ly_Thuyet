#!/usr/bin/env python3
"""
Kịch bản 7 (Mục 11.7): Kiểm thử hiệu năng (Performance Benchmark)
Khảo sát 4 cấu hình theo Bảng 11.13:
- P1: Replica Set 3 node | Ghi: {w: "majority", j: true} | Đọc: primary, "majority"
- P2: Replica Set 3 node | Ghi: {w: 1}                   | Đọc: primary, "local"
- P3: Replica Set 3 node | Ghi: {w: "majority", j: true} | Đọc: secondaryPreferred, "local"
- P4: Máy chủ đơn đối chứng | Ghi: {w: 1, j: true}       | Đọc: "local"

Hai loại tải:
- read_only: 100% tìm kiếm theo maCongDan chọn ngẫu nhiên đều
- mixed_80_20: 80% đọc theo maCongDan, 20% cập nhật diaChi và tăng phienBan

Số tác vụ đồng thời: 1, 5, 10 luồng (mô hình vòng kín).
Mặc định làm nóng 30s, đo 120s (có hỗ trợ cờ `--warmup` và `--duration` để điều chỉnh linh hoạt).
Xuất Bảng 11.16 ra results/kb7_results.md và results/kb7_results.json.
"""

import argparse
import json
import random
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from pymongo import ReadPreference, WriteConcern
from pymongo.read_concern import ReadConcern

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scenarios.common import (
    DB_NAME,
    SEED_VALUE,
    compute_latency_stats,
    get_rs_client,
    get_standalone_client,
    timed_operation,
)


CONFIG_PROFILES = {
    "P1": {
        "topology": "replica_set",
        "write_concern": WriteConcern(w="majority", j=True, wtimeout=5000),
        "read_concern": ReadConcern("majority"),
        "read_preference": ReadPreference.PRIMARY,
        "description": 'RS 3 node | w:"majority", j:true | primary, "majority"',
    },
    "P2": {
        "topology": "replica_set",
        "write_concern": WriteConcern(w=1, wtimeout=5000),
        "read_concern": ReadConcern("local"),
        "read_preference": ReadPreference.PRIMARY,
        "description": 'RS 3 node | w:1 | primary, "local"',
    },
    "P3": {
        "topology": "replica_set",
        "write_concern": WriteConcern(w="majority", j=True, wtimeout=5000),
        "read_concern": ReadConcern("local"),
        "read_preference": ReadPreference.SECONDARY_PREFERRED,
        "description": 'RS 3 node | w:"majority", j:true | secondaryPreferred, "local"',
    },
    "P4": {
        "topology": "standalone",
        "write_concern": WriteConcern(w=1, j=True, wtimeout=5000),
        "read_concern": ReadConcern("local"),
        "read_preference": ReadPreference.PRIMARY,
        "description": 'Standalone | w:1, j:true | "local"',
    },
}


def sample_primary_cpu_pct() -> float:
    try:
        res = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", "mongo1"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        val = res.stdout.strip().replace("%", "")
        return round(float(val), 2)
    except Exception:
        return 0.0


def run_single_benchmark_cell(
    profile_code: str,
    workload_type: str,
    concurrency: int,
    dataset_scale: int,
    warmup_sec: int,
    measure_sec: int,
) -> dict:
    prof = CONFIG_PROFILES[profile_code]
    print(
        f"  -> Đo [{profile_code}] | Tải: {workload_type} | Đồng thời: {concurrency} | "
        f"Quy mô: {dataset_scale:,} | Warmup: {warmup_sec}s | Đo: {measure_sec}s"
    )

    if prof["topology"] == "standalone":
        client = get_standalone_client(use_root=True)
    else:
        client = get_rs_client(use_root=False)

    coll = client.get_database(
        DB_NAME,
        write_concern=prof["write_concern"],
        read_concern=prof["read_concern"],
        read_preference=prof["read_preference"],
    )["congDan"]

    stop_flag = threading.Event()
    measuring_flag = threading.Event()
    thread_results = [{"reads_ms": [], "writes_ms": [], "errors": 0, "total": 0} for _ in range(concurrency)]

    def worker_fn(tid: int):
        rng = random.Random(SEED_VALUE + tid * 997)
        local_stat = thread_results[tid]
        while not stop_flag.is_set():
            is_measuring = measuring_flag.is_set()
            doc_num = rng.randint(1, dataset_scale)
            ma_cd = f"TEST{doc_num:06d}"

            if workload_type == "read_only" or rng.random() < 0.8:
                op_type = "read"
                rec = timed_operation(
                    f"{profile_code}_R",
                    "read",
                    lambda m=ma_cd: coll.find_one({"maCongDan": m}, {"_id": 1, "hoTen": 1, "diaChi": 1, "phienBan": 1}),
                )
            else:
                op_type = "write"
                new_q = f"Quan_{(doc_num % 5) + 1}"
                rec = timed_operation(
                    f"{profile_code}_W",
                    "write",
                    lambda m=ma_cd, q=new_q: coll.update_one(
                        {"maCongDan": m},
                        {"$set": {"diaChi.quan": q, "updatedAt": datetime.now(timezone.utc)}, "$inc": {"phienBan": 1}},
                    ),
                )

            if is_measuring and not stop_flag.is_set():
                local_stat["total"] += 1
                if rec["status"] == "success":
                    if op_type == "read":
                        local_stat["reads_ms"].append(rec["latency_ms"])
                    else:
                        local_stat["writes_ms"].append(rec["latency_ms"])
                else:
                    local_stat["errors"] += 1

    threads = [threading.Thread(target=worker_fn, args=(i,), daemon=True) for i in range(concurrency)]
    for t in threads:
        t.start()

    # Giai đoạn làm nóng (Warm-up)
    if warmup_sec > 0:
        time.sleep(warmup_sec)

    # Bắt đầu cửa sổ đo chính thức
    measuring_flag.set()
    t_measure_start = time.perf_counter()
    cpu_samples = []
    while time.perf_counter() - t_measure_start < measure_sec:
        cpu_samples.append(sample_primary_cpu_pct())
        time.sleep(1.0)
    actual_window_s = time.perf_counter() - t_measure_start
    measuring_flag.clear()
    stop_flag.set()

    for t in threads:
        t.join(timeout=5)
    client.close()

    all_reads = [x for tr in thread_results for x in tr["reads_ms"]]
    all_writes = [x for tr in thread_results for x in tr["writes_ms"]]
    total_sent = sum(tr["total"] for tr in thread_results)
    total_errors = sum(tr["errors"] for tr in thread_results)
    total_success = len(all_reads) + len(all_writes)

    throughput_ops = round(total_success / actual_window_s, 2) if actual_window_s > 0 else 0.0
    error_rate_pct = round((total_errors / total_sent) * 100.0, 3) if total_sent > 0 else 0.0
    avg_cpu = round(sum(cpu_samples) / len(cpu_samples), 2) if cpu_samples else 0.0

    return {
        "profile": profile_code,
        "workload": workload_type,
        "concurrency": concurrency,
        "dataset_scale": dataset_scale,
        "window_seconds": round(actual_window_s, 2),
        "throughput_ops_sec": throughput_ops,
        "total_success": total_success,
        "total_errors": total_errors,
        "error_rate_pct": error_rate_pct,
        "read_latency_ms": compute_latency_stats(all_reads),
        "write_latency_ms": compute_latency_stats(all_writes),
        "primary_cpu_avg_pct": avg_cpu,
    }


def run_kb7(
    profiles: list[str],
    concurrencies: list[int],
    workload: str = "mixed_80_20",
    scale: int = 1000,
    warmup_sec: int = 30,
    measure_sec: int = 120,
) -> dict:
    print("======================================================================")
    print("KỊCH BẢN 7: KIỂM THỬ HIỆU NĂNG - PERFORMANCE BENCHMARK (Mục 11.7)")
    print("======================================================================")

    rows = []
    for prof_code in profiles:
        for conc in concurrencies:
            try:
                cell = run_single_benchmark_cell(
                    profile_code=prof_code,
                    workload_type=workload,
                    concurrency=conc,
                    dataset_scale=scale,
                    warmup_sec=warmup_sec,
                    measure_sec=measure_sec,
                )
                rows.append(cell)
            except Exception as exc:
                print(f"  [WARN] Bỏ qua cấu hình {prof_code} (concurrency={conc}): {exc}")

    summary = {
        "scenario": "KB7_Performance",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "workload": workload,
        "dataset_scale": scale,
        "warmup_sec": warmup_sec,
        "measure_sec": measure_sec,
        "results": rows,
    }

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    with open(results_dir / "kb7_results.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str, ensure_ascii=False)

    md_lines = [
        f"# KẾT QUẢ KỊCH BẢN 7 – KIỂM THỬ HIỆU NĂNG (Bảng 11.16 - Tải `{workload}`, Bộ `{scale:,}` hồ sơ)",
        "",
        "| Cấu hình | Số tác vụ | Thông lượng (ops/s) | Đọc p50 / p95 (ms) | Ghi p50 / p95 (ms) | Ghi p99 (ms) | Lỗi (%) | CPU Primary (%) |",
        "| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    for r in rows:
        rl = r["read_latency_ms"]
        wl = r["write_latency_ms"]
        md_lines.append(
            f"| **{r['profile']}** | `{r['concurrency']}` | `{r['throughput_ops_sec']}` | "
            f"`{rl['p50_ms']} / {rl['p95_ms']}` | `{wl['p50_ms']} / {wl['p95_ms']}` | "
            f"`{wl['p99_ms']}` | `{r['error_rate_pct']}%` | `{r['primary_cpu_avg_pct']}%` |"
        )

    with open(results_dir / "kb7_results.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")

    print("[OK] Hoàn tất Kịch bản 7! Đã lưu kết quả tại: results/kb7_results.md")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profiles", nargs="+", default=["P1", "P2", "P3"])
    parser.add_argument("--concurrencies", nargs="+", type=int, default=[1, 5, 10])
    parser.add_argument("--workload", choices=["read_only", "mixed_80_20"], default="mixed_80_20")
    parser.add_argument("--scale", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=30, help="Thời gian làm nóng (mặc định 30s theo mục 11.7)")
    parser.add_argument("--duration", type=int, default=120, help="Thời gian cửa sổ đo (mặc định 120s theo mục 11.7)")
    args = parser.parse_args()
    run_kb7(
        profiles=args.profiles,
        concurrencies=args.concurrencies,
        workload=args.workload,
        scale=args.scale,
        warmup_sec=args.warmup,
        measure_sec=args.duration,
    )
