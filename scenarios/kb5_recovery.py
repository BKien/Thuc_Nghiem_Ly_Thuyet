#!/usr/bin/env python3
"""
Kịch bản 5 (Mục 11.5): Khôi phục node sau sự cố (Recovery)
Kiểm chứng 3 tình huống:
- Tình huống 5A: Bắt kịp qua oplog và priority takeover khi mongo1 (priority 2) quay lại.
  + Dừng mongo1, ghi phát sinh thao tác mới (mặc định 10.000 thao tác), ghi nhận cửa sổ oplog.
  + Khởi động lại mongo1 (mốc R0), quan sát chuỗi trạng thái (STARTUP -> RECOVERING -> SECONDARY -> PRIMARY),
    thời gian về SECONDARY (R1 - R0) và thời gian priority takeover về PRIMARY (R2 - R0).
- Tình huống 5B: Rollback của Primary cũ khi có thao tác ghi w: 1 chưa đạt đa số.
  + Cô lập mạng mongo1 khi đang là Primary, ghi các document KB5B_W1_... qua kết nối trực tiếp với w: 1.
  + Ghi các document KB5B_MOI_... với w: "majority" trên Primary mới.
  + Kết nối lại mạng cho mongo1, quan sát rollback và kiểm tra số document majority bị mất (= 0).
- Tình huống 5C: Đồng bộ lại từ dữ liệu trống (Initial Sync).
  + Xóa dữ liệu trên mongo3 và khởi động lại để quan sát trạng thái STARTUP2 -> SECONDARY và thời gian đồng bộ.
- Xuất Bảng 11.10 ra results/kb5_results.md và results/kb5_results.json.
"""

import argparse
import json
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from pymongo import WriteConcern
from pymongo.read_concern import ReadConcern

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scenarios.common import (
    DB_NAME,
    get_default_collection,
    get_direct_client,
    get_replication_window_and_lag,
    get_rs_client,
    timed_operation,
    wait_for_replication_catchup,
)
from scripts.fault_injector import inject_fault


def run_case_5a(num_missed_ops: int = 10000) -> dict:
    print(f"\n--- Tình huống 5A: Bắt kịp qua Oplog ({num_missed_ops:,} thao tác) & Priority Takeover ---")
    with get_rs_client(use_root=True) as client:
        wait_for_replication_catchup(client, timeout_sec=30)

    # Dừng mongo1
    inject_fault("stop", "mongo1")
    time.sleep(12)  # Chờ mongo2 hoặc mongo3 trở thành Primary

    base_dt = datetime(1995, 3, 10, tzinfo=timezone.utc)
    with get_dir_or_rs_active_primary() as p_client:
        cd = p_client.get_database(
            DB_NAME,
            write_concern=WriteConcern(w="majority", j=True, wtimeout=10000),
        )["congDan"]
        cd.delete_many({"maCongDan": {"$regex": "^KB5A_"}})

        print(f"[5A - Bước 1] Đang ghi {num_missed_ops:,} thao tác mới trong lúc mongo1 đang dừng...")
        for offset in range(0, num_missed_ops, 1000):
            batch_len = min(1000, num_missed_ops - offset)
            now_utc = datetime.now(timezone.utc)
            docs = [
                {
                    "maCongDan": f"KB5A_{offset + j + 1:06d}",
                    "hoTen": f"Cong dan KB5A {offset + j + 1:06d}",
                    "ngaySinh": base_dt,
                    "gioiTinh": "Nam",
                    "trangThai": "thu_nghiem",
                    "lienHe": {"soDienThoai": "0955000000"},
                    "hoGiaDinhId": None,
                    "diaChi": {"thanhPho": "Vung_A", "quan": "Quan_1", "phuong": "Phuong_1", "chiTiet": "5A"},
                    "phienBan": 1,
                    "updatedAt": now_utc,
                }
                for j in range(batch_len)
            ]
            cd.insert_many(docs, ordered=False)

        oplog_info = get_replication_window_and_lag(p_client)

    # Chạy tải nhẹ 1 thao tác mỗi 100ms để đo gián đoạn khi priority takeover
    stop_light_load = threading.Event()
    light_records = []

    def light_writer():
        with get_rs_client(use_root=False) as l_client:
            cd_l = get_default_collection(l_client, "congDan")
            seq = 0
            while not stop_light_load.is_set():
                seq += 1
                rec = timed_operation(
                    f"KB5A_LIGHT_{seq:04d}",
                    "update_light",
                    lambda: cd_l.update_one(
                        {"maCongDan": "TEST000001"},
                        {"$inc": {"phienBan": 1}, "$set": {"updatedAt": datetime.now(timezone.utc)}},
                    ),
                )
                light_records.append(rec)
                time.sleep(0.1)

    lw_thread = threading.Thread(target=light_writer, daemon=True)
    lw_thread.start()

    print("[5A - Bước 2] Khởi động lại mongo1 (mốc R0) và theo dõi trạng thái...")
    ev_start = inject_fault("start", "mongo1")
    r0_ns = ev_start["perf_counter_ns"]
    r1_ns = None
    r2_ns = None
    observed_states = []

    with get_direct_client("mongo2", use_root=True) as mon_client:
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                st = mon_client.admin.command("replSetGetStatus")
                m1 = next((m for m in st["members"] if m["name"].startswith("mongo1")), None)
                if m1:
                    s_str = m1.get("stateStr")
                    if not observed_states or observed_states[-1] != s_str:
                        observed_states.append(s_str)
                    if s_str in ("SECONDARY", "PRIMARY") and r1_ns is None and m1.get("health") == 1.0:
                        r1_ns = time.perf_counter_ns()
                    if s_str == "PRIMARY" and r2_ns is None:
                        r2_ns = time.perf_counter_ns()
                        break
            except Exception:
                pass
            time.sleep(0.25)

    time.sleep(2)
    stop_light_load.set()
    lw_thread.join(timeout=5)

    r1_minus_r0 = round((r1_ns - r0_ns) / 1e9, 2) if r1_ns else -1.0
    r2_minus_r0 = round((r2_ns - r0_ns) / 1e9, 2) if r2_ns else -1.0

    ok_light = [r for r in light_records if r["status"] == "success"]
    takeover_gap_s = 0.0
    for idx in range(1, len(ok_light)):
        gap = (ok_light[idx]["t1_ns"] - ok_light[idx - 1]["t1_ns"]) / 1e9
        if gap > takeover_gap_s:
            takeover_gap_s = gap
    takeover_pause_s = round(max(0.0, takeover_gap_s - 0.1), 2)

    with get_rs_client(use_root=True) as clean_client:
        wait_for_replication_catchup(clean_client, timeout_sec=30)
        get_default_collection(clean_client, "congDan").delete_many({"maCongDan": {"$regex": "^KB5A_"}})
        wait_for_replication_catchup(clean_client, timeout_sec=30)

    return {
        "missed_operations": num_missed_ops,
        "oplog_window_seconds": oplog_info["oplog_window_seconds"],
        "observed_states": observed_states,
        "r1_minus_r0_secondary_s": r1_minus_r0,
        "r2_minus_r0_primary_takeover_s": r2_minus_r0,
        "priority_takeover_write_pause_s": takeover_pause_s,
    }


def get_dir_or_rs_active_primary():
    for node_name in ["mongo2", "mongo3", "mongo1"]:
        try:
            cli = get_direct_client(node_name, use_root=True)
            h = cli.admin.command("hello")
            if h.get("isWritablePrimary"):
                return cli
            cli.close()
        except Exception:
            pass
    return get_rs_client(use_root=True)


def run_case_5b() -> dict:
    print("\n--- Tình huống 5B: Rollback của Primary cũ khi ghi w: 1 bị cô lập mạng ---")
    with get_rs_client(use_root=True) as client:
        wait_for_replication_catchup(client, timeout_sec=30)
        get_default_collection(client, "congDan").delete_many({"maCongDan": {"$regex": "^KB5B_"}})
        wait_for_replication_catchup(client, timeout_sec=20)

    base_dt = datetime(1995, 3, 10, tzinfo=timezone.utc)
    w1_confirmed = 0

    # Mở kết nối trực tiếp tới mongo1 trước khi cô lập mạng
    with get_direct_client("mongo1", use_root=False, timeout_ms=3000) as m1_direct:
        cd_w1 = m1_direct.get_database(DB_NAME, write_concern=WriteConcern(w=1, wtimeout=2000))["congDan"]
        inject_fault("isolate", "mongo1")

        # Ngay lập tức ghi các bản ghi w: 1 trên mongo1 trước khi nó step down
        for seq in range(1, 30):
            try:
                cd_w1.insert_one(
                    {
                        "maCongDan": f"KB5B_W1_{seq:04d}",
                        "hoTen": f"Cong dan Rollback W1 {seq:04d}",
                        "ngaySinh": base_dt,
                        "gioiTinh": "Nam",
                        "trangThai": "thu_nghiem",
                        "lienHe": {"soDienThoai": "0955111111"},
                        "hoGiaDinhId": None,
                        "diaChi": {"thanhPho": "Vung_A", "quan": "Quan_1", "phuong": "Phuong_1", "chiTiet": "5B_W1"},
                        "phienBan": 1,
                        "updatedAt": datetime.now(timezone.utc),
                    }
                )
                w1_confirmed += 1
            except Exception:
                break

    print(f"[5B - Bước 4] Số document w: 1 được xác nhận trên Primary cũ sau khi cô lập: {w1_confirmed}")

    # Chờ mongo2/mongo3 bầu Primary mới và ghi KB5B_MOI_... với majority
    time.sleep(13)
    majority_written = 20
    with get_dir_or_rs_active_primary() as new_p_client:
        cd_maj = new_p_client.get_database(
            DB_NAME,
            write_concern=WriteConcern(w="majority", j=True, wtimeout=10000),
        )["congDan"]
        for seq in range(1, majority_written + 1):
            cd_maj.insert_one(
                {
                    "maCongDan": f"KB5B_MOI_{seq:04d}",
                    "hoTen": f"Cong dan Majority {seq:04d}",
                    "ngaySinh": base_dt,
                    "gioiTinh": "Nu",
                    "trangThai": "thu_nghiem",
                    "lienHe": {"soDienThoai": "0955222222"},
                    "hoGiaDinhId": None,
                    "diaChi": {"thanhPho": "Vung_B", "quan": "Quan_2", "phuong": "Phuong_2", "chiTiet": "5B_MAJ"},
                    "phienBan": 1,
                    "updatedAt": datetime.now(timezone.utc),
                }
            )

    # Bước 6: Kết nối lại mạng cho mongo1 và chờ hội tụ
    print("[5B - Bước 6] Kết nối lại mạng cho mongo1 và kiểm tra Rollback...")
    inject_fault("reconnect", "mongo1")
    time.sleep(15)

    with get_rs_client(use_root=True) as rs_client:
        wait_for_replication_catchup(rs_client, timeout_sec=60)
        cd_check = get_default_collection(rs_client, "congDan")
        surviving_w1 = cd_check.count_documents({"maCongDan": {"$regex": "^KB5B_W1_"}})
        surviving_maj = cd_check.count_documents({"maCongDan": {"$regex": "^KB5B_MOI_"}})
        cd_check.delete_many({"maCongDan": {"$regex": "^KB5B_"}})
        wait_for_replication_catchup(rs_client, timeout_sec=20)

    rolled_back_count = max(0, w1_confirmed - surviving_w1)
    lost_majority = max(0, majority_written - surviving_maj)

    # Kiểm tra thư mục /data/db/rollback trong container mongo1
    rb_files_out = subprocess.run(
        ["docker", "exec", "mongo1", "sh", "-c", "find /data/db/rollback -type f 2>/dev/null | wc -l"],
        capture_output=True,
        text=True,
    )
    rb_file_count = int(rb_files_out.stdout.strip() or "0") if rb_files_out.returncode == 0 else 0

    return {
        "w1_confirmed_on_isolated_primary": w1_confirmed,
        "w1_rolled_back_docs": rolled_back_count,
        "rollback_bson_files_count": rb_file_count,
        "majority_written_on_new_primary": majority_written,
        "majority_lost_count": lost_majority,
    }


def run_case_5c() -> dict:
    print("\n--- Tình huống 5C: Đồng bộ lại từ volume trống (Initial Sync) trên mongo3 ---")
    inject_fault("stop", "mongo3")
    # Xóa nội dung dữ liệu trong /data/db của mongo3 bằng container tạm gắn cùng volume
    subprocess.run(
        ["docker", "run", "--rm", "-v", "mongo3_data:/data/db", "alpine", "sh", "-c", "rm -rf /data/db/*"],
        capture_output=True,
        text=True,
    )
    t_sync_start = time.perf_counter()
    inject_fault("start", "mongo3")

    states_5c = []
    with get_rs_client(use_root=True) as rs_client:
        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                st = rs_client.admin.command("replSetGetStatus")
                m3 = next((m for m in st["members"] if m["name"].startswith("mongo3")), None)
                if m3:
                    s_str = m3.get("stateStr")
                    if not states_5c or states_5c[-1] != s_str:
                        states_5c.append(s_str)
                    if s_str == "SECONDARY" and m3.get("health") == 1.0:
                        break
            except Exception:
                pass
            time.sleep(0.2)
        wait_for_replication_catchup(rs_client, timeout_sec=30)

    sync_duration_s = round(time.perf_counter() - t_sync_start, 2)
    print(f"[5C - Bước 7] Initial Sync trên mongo3 hoàn tất trong {sync_duration_s}s | Trạng thái: {states_5c}")
    return {
        "target_node": "mongo3",
        "observed_states": states_5c,
        "initial_sync_seconds_1000_docs": sync_duration_s,
    }


def run_kb5(missed_ops: int = 10000) -> dict:
    print("======================================================================")
    print("KỊCH BẢN 5: KHÔI PHỤC NODE SAU SỰ CỐ - RECOVERY (Mục 11.5)")
    print("======================================================================")

    res_5a = run_case_5a(num_missed_ops=missed_ops)
    res_5b = run_case_5b()
    res_5c = run_case_5c()

    summary = {
        "scenario": "KB5_Recovery",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "case_5A_oplog_catchup_and_takeover": res_5a,
        "case_5B_rollback": res_5b,
        "case_5C_initial_sync": res_5c,
    }

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    with open(results_dir / "kb5_results.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str, ensure_ascii=False)

    md_content = f"""# KẾT QUẢ KỊCH BẢN 5 – KHÔI PHỤC NODE SAU SỰ CỐ (Bảng 11.10)

| Hạng mục | Giá trị ghi nhận |
| :--- | :--- |
| **5A** – Số thao tác phát sinh khi `mongo1` dừng | `{res_5a['missed_operations']:,}` |
| **5A** – Cửa sổ oplog tại thời điểm khởi động lại | `{res_5a['oplog_window_seconds']} s` |
| **5A** – Chuỗi trạng thái của `mongo1` | `{' -> '.join(res_5a['observed_states'])}` |
| **5A** – `R1 - R0`: Thời gian trở về `SECONDARY` (s) | `{res_5a['r1_minus_r0_secondary_s']} s` |
| **5A** – `R2 - R0`: Thời gian đến `priority takeover` (s) | `{res_5a['r2_minus_r0_primary_takeover_s']} s` |
| **5A** – Gián đoạn ghi khi `priority takeover` (s) | `{res_5a['priority_takeover_write_pause_s']} s` |
| **5B** – Số document `w: 1` xác nhận trên Primary cũ khi cô lập | `{res_5b['w1_confirmed_on_isolated_primary']}` |
| **5B** – Số document `w: 1` bị rollback / số tệp BSON rollback | `{res_5b['w1_rolled_back_docs']} docs ({res_5b['rollback_bson_files_count']} files)` |
| **5B** – Số document `"majority"` bị mất | **`{res_5b['majority_lost_count']}`** (Kỳ vọng `0`) |
| **5C** – Chuỗi trạng thái & thời gian Initial Sync (1.000 hồ sơ) | `{' -> '.join(res_5c['observed_states'])}` (`{res_5c['initial_sync_seconds_1000_docs']} s`) |
"""
    with open(results_dir / "kb5_results.md", "w", encoding="utf-8") as f:
        f.write(md_content)

    print("[OK] Hoàn tất Kịch bản 5! Đã lưu kết quả tại: results/kb5_results.md")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--missed-ops", type=int, default=10000)
    args = parser.parse_args()
    run_kb5(missed_ops=args.missed_ops)
