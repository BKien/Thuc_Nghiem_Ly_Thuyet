#!/usr/bin/env python3
"""
Kịch bản 3 (Mục 11.3): Giả lập một node Secondary gặp sự cố
- Bước 1: Chạy tải nền đọc/ghi mỗi 50 ms để lấy mốc độ trễ bình thường trước sự cố.
- Bước 2: Xác định một node đang là SECONDARY (ví dụ mongo3).
- Bước 3: Gây sự cố bằng `docker kill <node>` (hoặc biến thể `docker network disconnect`) và ghi mốc T0.
- Bước 4: Lấy mẫu rs.status() từ Primary cho đến khi node lỗi có health: 0 (mốc T1); tính T1 - T0.
- Bước 5: Kiểm tra một thao tác ghi với {w: "majority"} (kỳ vọng thành công) và
          một thao tác với {w: 3, wtimeout: 5000} (kỳ vọng lỗi write concern sau ~5s).
- Bước 6: Duy trì tải trong lúc thiếu node để đo tỷ lệ lỗi và độ trễ p50/p95.
- Bước 7: Hỗ trợ biến thể cô lập mạng (`--mode isolate` hoặc chạy cả 2 chế độ `--mode both`).
- Xuất Bảng 11.7 ra results/kb3_results.md và results/kb3_results.json.
"""

import argparse
import json
import random
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
    get_default_collection,
    get_rs_client,
    save_operation_logs_csv,
    timed_operation,
    wait_for_replication_catchup,
)
from scripts.fault_injector import inject_fault


def run_single_kb3_variant(
    fault_mode: str = "kill",
    warmup_sec: int = 15,
    degraded_sec: int = 30,
    recover_after: bool = True,
) -> dict:
    """
    Thực thi 1 lượt Kịch bản 3 với `fault_mode` là 'kill' (dừng đột ngột) hoặc 'isolate' (cô lập mạng).
    """
    print(f"\n--- Đang chạy Kịch bản 3 (Chế độ: {fault_mode}) ---")
    with get_rs_client(use_root=True) as root_client:
        wait_for_replication_catchup(root_client, timeout_sec=30)
        status_before = root_client.admin.command("replSetGetStatus")
        term_before = status_before.get("term")
        sec_candidates = [
            m["name"].split(":")[0]
            for m in status_before["members"]
            if m.get("stateStr") == "SECONDARY"
        ]
        target_node = "mongo3" if "mongo3" in sec_candidates else sec_candidates[-1]

    stop_event = threading.Event()
    phase_marker = {"phase": "pre_fault"}
    write_records: list[dict] = []
    read_records: list[dict] = []

    def background_worker():
        rng = random.Random(SEED_VALUE)
        with get_rs_client(use_root=False) as w_client:
            cd = get_default_collection(w_client, "congDan")
            seq = 0
            while not stop_event.is_set():
                seq += 1
                cur_phase = phase_marker["phase"]
                target_id = f"TEST{rng.randint(1, 1000):06d}"

                w_rec = timed_operation(
                    f"KB3_{fault_mode}_W_{seq:05d}",
                    f"update_{cur_phase}",
                    lambda tid=target_id: cd.update_one(
                        {"maCongDan": tid},
                        {"$inc": {"phienBan": 1}, "$set": {"updatedAt": datetime.now(timezone.utc)}},
                    ),
                )
                write_records.append(w_rec)

                r_rec = timed_operation(
                    f"KB3_{fault_mode}_R_{seq:05d}",
                    f"find_{cur_phase}",
                    lambda tid=target_id: cd.find_one({"maCongDan": tid}, {"_id": 1, "phienBan": 1}),
                )
                read_records.append(r_rec)
                time.sleep(0.05)

    worker_thread = threading.Thread(target=background_worker, daemon=True)
    worker_thread.start()

    print(f"[Bước 1] Đang chạy tải nền ổn định trong {warmup_sec}s trước khi gây sự cố...")
    time.sleep(warmup_sec)

    # Bước 3: Gây sự cố trên Secondary
    print(f"[Bước 2 & 3] Gây sự cố '{fault_mode}' trên Secondary '{target_node}'...")
    phase_marker["phase"] = "post_fault"
    fault_ev = inject_fault(fault_mode, target_node)
    t0_ns = fault_ev["perf_counter_ns"]

    # Bước 4: Quan sát phát hiện lỗi qua rs.status()
    t1_ns = None
    with get_rs_client(use_root=True) as mon_client:
        deadline = time.time() + 45
        while time.time() < deadline:
            st = mon_client.admin.command("replSetGetStatus")
            m_target = next((m for m in st["members"] if m["name"].startswith(target_node)), None)
            if m_target and m_target.get("health") == 0.0:
                t1_ns = time.perf_counter_ns()
                break
            time.sleep(0.5)

        detection_time_s = round((t1_ns - t0_ns) / 1e9, 2) if t1_ns else -1.0
        print(f"[Bước 4] Đã phát hiện {target_node} health=0 sau T1 - T0 = {detection_time_s}s")

        # Bước 5: Kiểm tra ghi w: "majority" và w: 3
        print("[Bước 5] Kiểm tra thao tác ghi {w: 'majority'} và {w: 3, wtimeout: 5000}...")
        cd_maj = mon_client.get_database(
            DB_NAME,
            write_concern=WriteConcern(w="majority", j=True, wtimeout=5000),
            read_concern=ReadConcern("majority"),
        )["congDan"]
        cd_w3 = mon_client.get_database(
            DB_NAME,
            write_concern=WriteConcern(w=3, j=True, wtimeout=5000),
            read_concern=ReadConcern("majority"),
        )["congDan"]

        maj_probe = timed_operation(
            f"KB3_{fault_mode}_MAJ_PROBE",
            "write_majority",
            lambda: cd_maj.update_one(
                {"maCongDan": "TEST000001"},
                {"$inc": {"phienBan": 1}, "$set": {"updatedAt": datetime.now(timezone.utc)}},
            ),
        )
        w3_probe = timed_operation(
            f"KB3_{fault_mode}_W3_PROBE",
            "write_w3",
            lambda: cd_w3.update_one(
                {"maCongDan": "TEST000001"},
                {"$inc": {"phienBan": 1}, "$set": {"updatedAt": datetime.now(timezone.utc)}},
            ),
        )

        print(f"[Bước 6] Duy trì trạng thái thiếu node trong {degraded_sec}s...")
        time.sleep(degraded_sec)

        status_after = mon_client.admin.command("replSetGetStatus")
        term_after = status_after.get("term")
        new_election_occurred = term_after != term_before

    stop_event.set()
    worker_thread.join(timeout=5)

    if recover_after:
        recover_action = "start" if fault_mode == "kill" else "reconnect"
        print(f"[Khôi phục trạng thái chuẩn] Đang thực hiện '{recover_action}' cho {target_node}...")
        inject_fault(recover_action, target_node)
        with get_rs_client(use_root=True) as r_client:
            wait_for_replication_catchup(r_client, timeout_sec=60)

    save_operation_logs_csv(write_records + read_records, f"kb3_{fault_mode}_operations.csv")

    pre_writes = [r["latency_ms"] for r in write_records if r["op_type"] == "update_pre_fault" and r["status"] == "success"]
    post_all = [r for r in write_records if r["op_type"] == "update_post_fault"]
    post_writes_ok = [r["latency_ms"] for r in post_all if r["status"] == "success"]
    post_errors = sum(1 for r in post_all if r["status"] != "success")
    error_rate_pct = round((post_errors / len(post_all)) * 100.0, 2) if post_all else 0.0

    pre_stats = compute_latency_stats(pre_writes)
    post_stats = compute_latency_stats(post_writes_ok)

    return {
        "fault_mode": fault_mode,
        "target_node": target_node,
        "detection_time_s": detection_time_s,
        "new_election_occurred": new_election_occurred,
        "majority_error_rate_pct": error_rate_pct,
        "pre_fault_write_latency_ms": pre_stats,
        "post_fault_write_latency_ms": post_stats,
        "majority_probe_result": {"status": maj_probe["status"], "latency_ms": maj_probe["latency_ms"]},
        "w3_probe_result": {
            "status": w3_probe["status"],
            "latency_ms": w3_probe["latency_ms"],
            "error": w3_probe["error"],
        },
    }


def run_kb3(mode: str = "both", warmup_sec: int = 15, degraded_sec: int = 30) -> dict:
    print("======================================================================")
    print("KỊCH BẢN 3: GIẢ LẬP MỘT NODE SECONDARY GẶP SỰ CỐ (Mục 11.3)")
    print("======================================================================")

    results = {}
    if mode in ("kill", "both"):
        results["kill"] = run_single_kb3_variant("kill", warmup_sec, degraded_sec, recover_after=True)
    if mode in ("isolate", "both"):
        results["isolate"] = run_single_kb3_variant("isolate", warmup_sec, degraded_sec, recover_after=True)

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    with open(results_dir / "kb3_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str, ensure_ascii=False)

    k_res = results.get("kill", {})
    i_res = results.get("isolate", {})
    k_pre = k_res.get("pre_fault_write_latency_ms", {})
    k_post = k_res.get("post_fault_write_latency_ms", {})
    i_pre = i_res.get("pre_fault_write_latency_ms", {})
    i_post = i_res.get("post_fault_write_latency_ms", {})

    md_content = f"""# KẾT QUẢ KỊCH BẢN 3 – GIẢ LẬP MỘT NODE SECONDARY GẶP SỰ CỐ (Bảng 11.7)

| Hạng mục | Dừng đột ngột (`docker kill`) | Cô lập mạng (`docker network disconnect`) |
| :--- | :---: | :---: |
| Node bị gây sự cố | `{k_res.get('target_node', 'N/A')}` | `{i_res.get('target_node', 'N/A')}` |
| Thời gian phát hiện `T1 - T0` (s) | `{k_res.get('detection_time_s', 'N/A')} s` | `{i_res.get('detection_time_s', 'N/A')} s` |
| Có bầu chọn mới không | `{'Có' if k_res.get('new_election_occurred') else 'Không'}` | `{'Có' if i_res.get('new_election_occurred') else 'Không'}` |
| Tỷ lệ lỗi ghi `"majority"` (%) | `{k_res.get('majority_error_rate_pct', 'N/A')} %` | `{i_res.get('majority_error_rate_pct', 'N/A')} %` |
| Độ trễ ghi p50 / p95 trước sự cố (ms) | `{k_pre.get('p50_ms', 'N/A')} / {k_pre.get('p95_ms', 'N/A')}` | `{i_pre.get('p50_ms', 'N/A')} / {i_pre.get('p95_ms', 'N/A')}` |
| Độ trễ ghi p50 / p95 trong lúc thiếu node (ms) | `{k_post.get('p50_ms', 'N/A')} / {k_post.get('p95_ms', 'N/A')}` | `{i_post.get('p50_ms', 'N/A')} / {i_post.get('p95_ms', 'N/A')}` |
| Kết quả thao tác `w: 3` | `{k_res.get('w3_probe_result', {}).get('status', 'N/A')} ({k_res.get('w3_probe_result', {}).get('latency_ms', 'N/A')} ms)` | `{i_res.get('w3_probe_result', {}).get('status', 'N/A')} ({i_res.get('w3_probe_result', {}).get('latency_ms', 'N/A')} ms)` |
"""
    with open(results_dir / "kb3_results.md", "w", encoding="utf-8") as f:
        f.write(md_content)

    print("[OK] Hoàn tất Kịch bản 3! Đã lưu kết quả tại: results/kb3_results.md")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["kill", "isolate", "both"], default="both")
    parser.add_argument("--warmup", type=int, default=15)
    parser.add_argument("--duration", type=int, default=30)
    args = parser.parse_args()
    run_kb3(mode=args.mode, warmup_sec=args.warmup, degraded_sec=args.duration)
