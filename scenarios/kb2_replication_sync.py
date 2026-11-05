#!/usr/bin/env python3
"""
Kịch bản 2 (Mục 11.2): Kiểm tra đồng bộ/phân phối dữ liệu giữa các node
- Bước 1: Ghi nhận mốc ban đầu (cửa sổ oplog, độ trễ sao chép = 0, số document từng collection).
- Bước 2: Đo độ trễ của từng thay đổi (200 mẫu) ghi với w: 1 trên Primary (t0),
          polling mỗi 5 ms trên từng Secondary cho đến khi thấy (t1), tính t1 - t0.
- Bước 3: Ghi theo lô với cường độ tăng dần (1.000, 10.000, 50.000 document bằng insert_many
          kích thước 1.000 doc/lần gửi), lấy mẫu độ trễ và thời gian hội tụ.
- Bước 4: Đối chiếu sau khi ngừng ghi trên cả 3 node: số document, số index, và dbHash.
- Bước 5: Dọn dữ liệu KB2_ và xác nhận số lượng trở về ban đầu.
- Xuất Bảng 11.4, 11.5, 11.6 ra results/kb2_results.md và results/kb2_results.json.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pymongo import ReadPreference, WriteConcern

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scenarios.common import (
    DB_NAME,
    NODE_NAMES,
    compute_latency_stats,
    get_direct_client,
    get_node_db_snapshot,
    get_replication_window_and_lag,
    get_rs_client,
    wait_for_replication_catchup,
)


def run_kb2(samples: int = 200, batch_sizes: list[int] | None = None) -> dict:
    if batch_sizes is None:
        batch_sizes = [1000, 10000, 50000]

    print("======================================================================")
    print("KỊCH BẢN 2: KIỂM TRA ĐỒNG BỘ DỮ LIỆU GIỮA CÁC NODE (Mục 11.2)")
    print("======================================================================")

    with get_rs_client(use_root=True) as root_client:
        wait_for_replication_catchup(root_client, timeout_sec=30)
        baseline_repl = get_replication_window_and_lag(root_client)
        status = root_client.admin.command("replSetGetStatus")
        sec_members = [m["name"].split(":")[0] for m in status["members"] if m["stateStr"] == "SECONDARY"]

    print(f"[Bước 1] Cửa sổ Oplog ban đầu: {baseline_repl['oplog_window_seconds']}s | Secondary nodes: {sec_members}")

    # Mở kết nối trực tiếp tới các Secondary và kết nối Replica Set
    sec_clients = {name: get_direct_client(name) for name in sec_members}
    sec_colls = {
        name: cli.get_database(DB_NAME, read_preference=ReadPreference.SECONDARY_PREFERRED)["congDan"]
        for name, cli in sec_clients.items()
    }

    per_doc_lag_ms = {name: [] for name in sec_members}
    base_dt = datetime(1997, 1, 1, tzinfo=timezone.utc)

    with get_rs_client(use_root=False) as rs_client:
        # Dùng w: 1 ở Bước 2 để thời điểm xác nhận không phụ thuộc Secondary đã ghi nhận hay chưa (Mục 11.2)
        cd_w1 = rs_client.get_database(DB_NAME, write_concern=WriteConcern(w=1))["congDan"]
        cd_maj = rs_client.get_database(DB_NAME, write_concern=WriteConcern(w="majority", j=True))["congDan"]
        cd_maj.delete_many({"maCongDan": {"$regex": "^KB2_"}})
        wait_for_replication_catchup(rs_client, timeout_sec=20)

        print(f"[Bước 2] Đang đo độ trễ sao chép từng thay đổi trên {samples} mẫu (w: 1, chu kỳ đọc 5 ms)...")
        for s_idx in range(1, samples + 1):
            ma_cd = f"KB2_SINGLE_{s_idx:06d}"
            now_utc = datetime.now(timezone.utc)
            doc = {
                "maCongDan": ma_cd,
                "hoTen": f"Cong dan KB2 Single {s_idx:06d}",
                "ngaySinh": base_dt,
                "gioiTinh": "Nam",
                "trangThai": "thu_nghiem",
                "lienHe": {"soDienThoai": "0922000000"},
                "hoGiaDinhId": None,
                "diaChi": {"thanhPho": "Vung_A", "quan": "Quan_1", "phuong": "Phuong_1", "chiTiet": "KB2"},
                "phienBan": 1,
                "updatedAt": now_utc,
            }
            cd_w1.insert_one(doc)
            t0_ns = time.perf_counter_ns()

            for sec_name, s_coll in sec_colls.items():
                deadline = time.perf_counter_ns() + int(10 * 1e9)
                while time.perf_counter_ns() < deadline:
                    if s_coll.find_one({"maCongDan": ma_cd}, {"_id": 1}) is not None:
                        t1_ns = time.perf_counter_ns()
                        per_doc_lag_ms[sec_name].append(round((t1_ns - t0_ns) / 1e6, 3))
                        break
                    time.sleep(0.005)

        cd_maj.delete_many({"maCongDan": {"$regex": "^KB2_SINGLE_"}})
        wait_for_replication_catchup(rs_client, timeout_sec=30)

        # Bước 3: Ghi theo lô (1.000, 10.000, 50.000 document)
        batch_results = []
        for b_size in batch_sizes:
            print(f"[Bước 3] Đang ghi lô {b_size:,} document (mỗi lần gửi 1.000 document)...")
            cd_maj.delete_many({"maCongDan": {"$regex": "^KB2_BATCH_"}})
            wait_for_replication_catchup(rs_client, timeout_sec=30)

            max_lag_during_write = 0.0
            t_batch_start = time.perf_counter()

            for offset in range(0, b_size, 1000):
                chunk_size = min(1000, b_size - offset)
                now_utc = datetime.now(timezone.utc)
                docs = [
                    {
                        "maCongDan": f"KB2_BATCH_{b_size}_{offset + j + 1:06d}",
                        "hoTen": f"Cong dan KB2 Batch {offset + j + 1:06d}",
                        "ngaySinh": base_dt,
                        "gioiTinh": "Nu" if j % 2 else "Nam",
                        "trangThai": "thu_nghiem",
                        "lienHe": {"soDienThoai": "0922111111"},
                        "hoGiaDinhId": None,
                        "diaChi": {"thanhPho": "Vung_B", "quan": "Quan_2", "phuong": "Phuong_2", "chiTiet": "Batch"},
                        "phienBan": 1,
                        "updatedAt": now_utc,
                    }
                    for j in range(chunk_size)
                ]
                cd_w1.insert_many(docs, ordered=False)
                lag_info = get_replication_window_and_lag(rs_client)
                if lag_info["max_secondary_lag_seconds"] > max_lag_during_write:
                    max_lag_during_write = lag_info["max_secondary_lag_seconds"]

            write_duration_s = round(time.perf_counter() - t_batch_start, 3)

            # Đo thời gian hội tụ sau khi lô kết thúc
            t_conv_start = time.perf_counter()
            while True:
                all_caught_up = all(
                    s_coll.count_documents({"maCongDan": {"$regex": f"^KB2_BATCH_{b_size}_"}}) == b_size
                    for s_coll in sec_colls.values()
                )
                if all_caught_up:
                    break
                if time.perf_counter() - t_conv_start > 120:
                    break
                time.sleep(0.05)
            convergence_s = round(time.perf_counter() - t_conv_start, 3)

            batch_results.append(
                {
                    "batch_size": b_size,
                    "write_duration_s": write_duration_s,
                    "max_lag_during_write_s": max_lag_during_write,
                    "convergence_time_s": convergence_s,
                }
            )

        # Bước 4: Đối chiếu sau khi ngừng ghi (số document, danh sách index, dbHash)
        print("[Bước 4] Đang đối chiếu số lượng document, index và dbHash trên cả 3 node...")
        wait_for_replication_catchup(rs_client, timeout_sec=60)
        snapshots = {NODE_NAMES[idx]: get_node_db_snapshot(idx) for idx in range(3)}

        # Bước 5: Dọn dữ liệu KB2_
        print("[Bước 5] Dọn dẹp dữ liệu thử nghiệm KB2_ và chờ đồng bộ...")
        cd_maj.delete_many({"maCongDan": {"$regex": "^KB2_"}})
        wait_for_replication_catchup(rs_client, timeout_sec=60)
        post_cleanup_counts = {
            NODE_NAMES[idx]: get_node_db_snapshot(idx)["counts"]["congDan"] for idx in range(3)
        }

    for cli in sec_clients.values():
        cli.close()

    lag_stats = {sec: compute_latency_stats(vals) for sec, vals in per_doc_lag_ms.items()}

    result = {
        "scenario": "KB2_Replication_Sync",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_oplog": baseline_repl,
        "table_11_4_single_change_lag_ms": lag_stats,
        "table_11_5_batch_replication": batch_results,
        "table_11_6_node_snapshots": snapshots,
        "post_cleanup_congDan_counts": post_cleanup_counts,
    }

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    with open(results_dir / "kb2_results.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str, ensure_ascii=False)

    sec_keys = list(lag_stats.keys())
    s1 = lag_stats[sec_keys[0]] if len(sec_keys) > 0 else {}
    s2 = lag_stats[sec_keys[1]] if len(sec_keys) > 1 else {}
    n1, n2, n3 = snapshots["mongo1"], snapshots["mongo2"], snapshots["mongo3"]

    md_lines = [
        "# KẾT QUẢ KỊCH BẢN 2 – KIỂM TRA ĐỒNG BỘ DỮ LIỆU GIỮA CÁC NODE (Mục 11.2)",
        "",
        f"## Bảng 11.4. Độ trễ sao chép theo từng thay đổi ({samples} mẫu)",
        f"| Chỉ số (ms) | {sec_keys[0] if len(sec_keys)>0 else 'Sec1'} | {sec_keys[1] if len(sec_keys)>1 else 'Sec2'} |",
        "| :--- | :---: | :---: |",
        f"| Trung vị (p50) | `{s1.get('p50_ms')}` | `{s2.get('p50_ms')}` |",
        f"| p95 | `{s1.get('p95_ms')}` | `{s2.get('p95_ms')}` |",
        f"| p99 | `{s1.get('p99_ms')}` | `{s2.get('p99_ms')}` |",
        f"| Lớn nhất | `{s1.get('max_ms')}` | `{s2.get('max_ms')}` |",
        "",
        "## Bảng 11.5. Độ trễ sao chép khi ghi theo lô",
        "| Kích thước lô | Thời gian ghi lô (s) | Độ trễ lớn nhất trong lúc ghi (s) | Thời gian hội tụ sau lô (s) |",
        "| :---: | :---: | :---: | :---: |",
    ]
    for br in batch_results:
        md_lines.append(
            f"| `{br['batch_size']:,}` | `{br['write_duration_s']}` | `{br['max_lag_during_write_s']}` | `{br['convergence_time_s']}` |"
        )

    md_lines.extend(
        [
            "",
            "## Bảng 11.6. Đối chiếu dữ liệu giữa ba node sau khi ngừng ghi",
            "| Hạng mục | mongo1 | mongo2 | mongo3 |",
            "| :--- | :---: | :---: | :---: |",
            f"| Số document `congDan` | `{n1['counts']['congDan']}` | `{n2['counts']['congDan']}` | `{n3['counts']['congDan']}` |",
            f"| Số index `congDan` | `{n1['index_counts']['congDan']}` | `{n2['index_counts']['congDan']}` | `{n3['index_counts']['congDan']}` |",
            f"| `dbHash` của `congDan` | `{n1['dbHash_collections'].get('congDan')}` | `{n2['dbHash_collections'].get('congDan')}` | `{n3['dbHash_collections'].get('congDan')}` |",
            f"| Số document sau dọn dẹp `KB2_` | `{post_cleanup_counts['mongo1']}` | `{post_cleanup_counts['mongo2']}` | `{post_cleanup_counts['mongo3']}` |",
        ]
    )

    with open(results_dir / "kb2_results.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")

    print("[OK] Hoàn tất Kịch bản 2! Đã lưu kết quả tại: results/kb2_results.md")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--quick", action="store_true", help="Chạy nhanh với lô 1.000 và 5.000")
    args = parser.parse_args()
    batches = [1000, 5000] if args.quick else [1000, 10000, 50000]
    run_kb2(samples=args.samples, batch_sizes=batches)
