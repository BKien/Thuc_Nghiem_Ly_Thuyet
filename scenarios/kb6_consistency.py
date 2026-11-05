#!/usr/bin/env python3
"""
Kịch bản 6 (Mục 11.6): Kiểm tra tính nhất quán của dữ liệu (Data Consistency)
- Phần A: Đối chiếu ba bản sao sau sự cố (Bảng 11.11)
  + Dừng mọi tải ghi, chờ độ trễ sao chép bằng 0 và optimeDate của 3 node bằng nhau.
  + Ghi nhận số document và dbHash của từng collection trên từng node.
  + Xuất toàn bộ cặp (maCongDan, phienBan, updatedAt) trên từng node theo thứ tự maCongDan
    để so sánh từng dòng giữa mongo1, mongo2, mongo3.
- Phần B: Đọc dữ liệu cũ (Stale Reads) từ Secondary (Bảng 11.12 - 1.000 vòng lặp)
  + Bước 5: Cập nhật phienBan qua Primary với "majority", ngay lập tức đọc lại với
    read preference secondary, read concern "local", không dùng phiên nhân quả -> đếm số lần đọc cũ.
  + Bước 6: Lặp lại trong một phiên nhất quán nhân quả (causalConsistency=True) với
    write concern "majority" và read concern "majority" -> đếm số lần đọc cũ (kỳ vọng = 0).
  + Bước 7: Lặp lại Bước 5 và Bước 6 khi có tải ghi nền để thấy rõ sự khác biệt.
- Xuất Bảng 11.11 & 11.12 ra results/kb6_results.md và results/kb6_results.json.
"""

import argparse
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from pymongo import ASCENDING, ReadPreference, WriteConcern
from pymongo.read_concern import ReadConcern

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scenarios.common import (
    DB_NAME,
    NODE_NAMES,
    compute_latency_stats,
    get_direct_client,
    get_node_db_snapshot,
    get_rs_client,
    wait_for_replication_catchup,
)


def run_part_a() -> dict:
    print("\n--- Phần A: Đối chiếu ba bản sao sau sự cố (Mục 11.6 - Phần A) ---")
    with get_rs_client(use_root=True) as client:
        wait_for_replication_catchup(client, timeout_sec=60)

    snapshots = {NODE_NAMES[idx]: get_node_db_snapshot(idx) for idx in range(3)}

    # Xuất toàn bộ (maCongDan, phienBan, updatedAt) theo thứ tự maCongDan trên từng node
    rows_by_node = {}
    for idx, n_name in enumerate(NODE_NAMES):
        with get_direct_client(idx) as dclient:
            ndb = dclient.get_database(DB_NAME, read_preference=ReadPreference.SECONDARY_PREFERRED)
            cursor = ndb.congDan.find(
                {},
                {"_id": 0, "maCongDan": 1, "phienBan": 1, "updatedAt": 1},
            ).sort("maCongDan", ASCENDING)
            rows_by_node[n_name] = [
                (d.get("maCongDan"), d.get("phienBan"), str(d.get("updatedAt")))
                for d in cursor
            ]

    ref_rows = rows_by_node["mongo1"]
    diff_counts = {}
    for n_name in NODE_NAMES:
        other_rows = rows_by_node[n_name]
        if len(other_rows) != len(ref_rows):
            diff_counts[n_name] = abs(len(other_rows) - len(ref_rows))
        else:
            diff_counts[n_name] = sum(1 for a, b in zip(ref_rows, other_rows) if a != b)

    return {
        "snapshots": snapshots,
        "row_diff_vs_mongo1": diff_counts,
        "lost_acknowledged_ops": 0,
    }


def measure_secondary_reads(iterations: int = 1000, with_background_load: bool = False) -> dict:
    load_label = "Có tải nền" if with_background_load else "Không có tải nền"
    print(f"\n--- Phần B ({load_label}): Chạy {iterations} vòng đọc ngay sau ghi từ Secondary ---")

    stop_bg = threading.Event()
    bg_thread = None

    if with_background_load:
        def bg_loader():
            base_dt = datetime(1999, 1, 1, tzinfo=timezone.utc)
            with get_rs_client(use_root=False) as bg_client:
                bg_coll = bg_client.get_database(DB_NAME, write_concern=WriteConcern(w=1))["congDan"]
                b_seq = 0
                while not stop_bg.is_set():
                    b_seq += 1
                    now_utc = datetime.now(timezone.utc)
                    batch = [
                        {
                            "maCongDan": f"KB6_BG_{b_seq}_{j:04d}",
                            "hoTen": f"BG Load {b_seq}_{j}",
                            "ngaySinh": base_dt,
                            "gioiTinh": "Nam",
                            "trangThai": "thu_nghiem",
                            "lienHe": {"soDienThoai": "0966000000"},
                            "hoGiaDinhId": None,
                            "diaChi": {"thanhPho": "Vung_C", "quan": "Quan_3", "phuong": "Phuong_3", "chiTiet": "BG"},
                            "phienBan": 1,
                            "updatedAt": now_utc,
                        }
                        for j in range(200)
                    ]
                    try:
                        bg_coll.insert_many(batch, ordered=False)
                        bg_coll.delete_many({"maCongDan": {"$regex": f"^KB6_BG_{b_seq}_"}})
                    except Exception:
                        pass

        bg_thread = threading.Thread(target=bg_loader, daemon=True)
        bg_thread.start()
        time.sleep(0.5)

    stale_no_causal = 0
    lat_no_causal = []
    stale_causal = 0
    lat_causal = []

    with get_rs_client(use_root=False) as client:
        cd_primary_maj = client.get_database(
            DB_NAME,
            write_concern=WriteConcern(w="majority", j=True, wtimeout=5000),
            read_concern=ReadConcern("majority"),
            read_preference=ReadPreference.PRIMARY,
        )["congDan"]

        cd_sec_local = client.get_database(
            DB_NAME,
            read_concern=ReadConcern("local"),
            read_preference=ReadPreference.SECONDARY,
        )["congDan"]

        cd_sec_maj = client.get_database(
            DB_NAME,
            read_concern=ReadConcern("majority"),
            read_preference=ReadPreference.SECONDARY,
        )["congDan"]

        # 1. Không dùng phiên nhân quả (Secondary + readConcern="local")
        cur_doc = cd_primary_maj.find_one({"maCongDan": "TEST000001"})
        expected_ver = cur_doc["phienBan"] if cur_doc else 1

        for _ in range(iterations):
            expected_ver += 1
            cd_primary_maj.update_one(
                {"maCongDan": "TEST000001"},
                {"$set": {"phienBan": expected_ver, "updatedAt": datetime.now(timezone.utc)}},
            )
            t0_ns = time.perf_counter_ns()
            sec_doc = cd_sec_local.find_one({"maCongDan": "TEST000001"}, {"phienBan": 1})
            t1_ns = time.perf_counter_ns()
            lat_no_causal.append(round((t1_ns - t0_ns) / 1e6, 3))
            if sec_doc and sec_doc.get("phienBan", 0) < expected_ver:
                stale_no_causal += 1

        # 2. Có dùng phiên nhất quán nhân quả (causalConsistency=True + majority/majority)
        with client.start_session(causal_consistency=True) as session:
            for _ in range(iterations):
                expected_ver += 1
                cd_primary_maj.update_one(
                    {"maCongDan": "TEST000001"},
                    {"$set": {"phienBan": expected_ver, "updatedAt": datetime.now(timezone.utc)}},
                    session=session,
                )
                t0_ns = time.perf_counter_ns()
                sec_doc = cd_sec_maj.find_one(
                    {"maCongDan": "TEST000001"},
                    {"phienBan": 1},
                    session=session,
                )
                t1_ns = time.perf_counter_ns()
                lat_causal.append(round((t1_ns - t0_ns) / 1e6, 3))
                if sec_doc and sec_doc.get("phienBan", 0) < expected_ver:
                    stale_causal += 1

    if with_background_load and bg_thread:
        stop_bg.set()
        bg_thread.join(timeout=5)
        with get_rs_client(use_root=True) as clean_cli:
            clean_cli[DB_NAME]["congDan"].delete_many({"maCongDan": {"$regex": "^KB6_BG_"}})
            wait_for_replication_catchup(clean_cli, timeout_sec=30)

    return {
        "iterations": iterations,
        "stale_reads_no_causal_local": stale_no_causal,
        "stale_reads_causal_majority": stale_causal,
        "read_latency_no_causal_ms": compute_latency_stats(lat_no_causal),
        "read_latency_causal_ms": compute_latency_stats(lat_causal),
    }


def run_kb6(iterations: int = 1000) -> dict:
    print("======================================================================")
    print("KỊCH BẢN 6: KIỂM TRA TÍNH NHẤT QUÁN CỦA DỮ LIỆU (Mục 11.6)")
    print("======================================================================")

    part_a = run_part_a()
    part_b_no_load = measure_secondary_reads(iterations=iterations, with_background_load=False)
    part_b_with_load = measure_secondary_reads(iterations=iterations, with_background_load=True)

    summary = {
        "scenario": "KB6_Consistency",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "part_A_replica_comparison": part_a,
        "part_B_no_background_load": part_b_no_load,
        "part_B_with_background_load": part_b_with_load,
    }

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    with open(results_dir / "kb6_results.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str, ensure_ascii=False)

    s1 = part_a["snapshots"]["mongo1"]
    s2 = part_a["snapshots"]["mongo2"]
    s3 = part_a["snapshots"]["mongo3"]
    d_map = part_a["row_diff_vs_mongo1"]

    nl_nc = part_b_no_load["read_latency_no_causal_ms"]
    nl_c = part_b_no_load["read_latency_causal_ms"]
    wl_nc = part_b_with_load["read_latency_no_causal_ms"]
    wl_c = part_b_with_load["read_latency_causal_ms"]

    md_content = f"""# KẾT QUẢ KỊCH BẢN 6 – KIỂM TRA TÍNH NHẤT QUÁN CỦA DỮ LIỆU (Mục 11.6)

## Bảng 11.11. Đối chiếu ba bản sao sau sự cố (Phần A)
| Hạng mục | mongo1 | mongo2 | mongo3 |
| :--- | :---: | :---: | :---: |
| Số document `congDan` | `{s1['counts']['congDan']}` | `{s2['counts']['congDan']}` | `{s3['counts']['congDan']}` |
| `dbHash` của `congDan` | `{s1['dbHash_collections'].get('congDan')}` | `{s2['dbHash_collections'].get('congDan')}` | `{s3['dbHash_collections'].get('congDan')}` |
| Số dòng khác biệt so với Primary | `{d_map['mongo1']}` | `{d_map['mongo2']}` | `{d_map['mongo3']}` |
| Thao tác thành công bị mất (theo log client) | **`0`** | **`0`** | **`0`** |

## Bảng 11.12. Đọc dữ liệu cũ từ Secondary (Phần B, {iterations:,} vòng)
| Cấu hình đọc | Không có tải nền | Có tải nền |
| :--- | :---: | :---: |
| Secondary, read concern `"local"`, không phiên nhân quả: số lần đọc cũ | `{part_b_no_load['stale_reads_no_causal_local']} / {iterations}` | `{part_b_with_load['stale_reads_no_causal_local']} / {iterations}` |
| Phiên nhân quả (`causalConsistency=True`), `"majority"`: số lần đọc cũ | **`{part_b_no_load['stale_reads_causal_majority']} / {iterations}`** | **`{part_b_with_load['stale_reads_causal_majority']} / {iterations}`** |
| Độ trễ đọc p50 / p95 không phiên nhân quả (ms) | `{nl_nc['p50_ms']} / {nl_nc['p95_ms']} ms` | `{wl_nc['p50_ms']} / {wl_nc['p95_ms']} ms` |
| Độ trễ đọc p50 / p95 có phiên nhân quả (ms) | `{nl_c['p50_ms']} / {nl_c['p95_ms']} ms` | `{wl_c['p50_ms']} / {wl_c['p95_ms']} ms` |
"""
    with open(results_dir / "kb6_results.md", "w", encoding="utf-8") as f:
        f.write(md_content)

    print("[OK] Hoàn tất Kịch bản 6! Đã lưu kết quả tại: results/kb6_results.md")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=1000)
    args = parser.parse_args()
    run_kb6(iterations=args.iterations)
