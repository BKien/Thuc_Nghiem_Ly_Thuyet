#!/usr/bin/env python3
"""
Kịch bản 4 (Mục 11.4): Kiểm tra Failover khi Primary ngừng hoạt động
- Bước 1: Chạy tải ghi có đánh số liên tục KB4_000001, KB4_000002... mỗi 20 ms
          với write concern {w: "majority", j: true, wtimeout: 5000}.
- Bước 2: Xác định Primary hiện tại (thường là mongo1) và thực hiện gây sự cố (T0).
- Bước 3: Lấy mẫu rs.status() từ một Secondary để ghi nhận thời điểm có Primary mới (T2),
          tên node được bầu và term mới.
- Bước 4: Duy trì tải ghi sau failover.
- Bước 5: Tính toán từ log client:
          + T1: Thời điểm hoàn tất thao tác ghi thành công cuối cùng trước gián đoạn
          + T3: Thời điểm hoàn tất thao tác ghi thành công đầu tiên trên Primary mới
          + Thời gian bầu chọn: T2 - T0
          + Thời gian gián đoạn ghi phía ứng dụng: T3 - T1
          + Thống kê 4 nhóm trạng thái thao tác.
- Bước 6: Kiểm tra mất dữ liệu: đối chiếu tập maCongDan đã xác nhận thành công với dữ liệu trên Primary mới.
- Bước 7: Hỗ trợ 3 biến thể của Bảng 11.9:
          (1) docker kill, retryWrites=true
          (2) docker kill, retryWrites=false
          (3) docker stop, retryWrites=true
- Xuất Bảng 11.9 ra results/kb4_results.md và results/kb4_results.json.
"""

import argparse
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scenarios.common import (
    DB_NAME,
    get_default_collection,
    get_direct_client,
    get_rs_client,
    save_operation_logs_csv,
    timed_operation,
    wait_for_replication_catchup,
)
from scripts.fault_injector import inject_fault


def run_single_failover_trial(
    variant_name: str,
    fault_action: str = "kill",
    retry_writes: bool = True,
    pre_sec: int = 10,
    post_sec: int = 15,
    restore_after: bool = True,
) -> dict:
    print(f"\n--- Đang chạy Kịch bản 4 [{variant_name}]: action={fault_action}, retryWrites={retry_writes} ---")

    with get_rs_client(use_root=True) as root_client:
        wait_for_replication_catchup(root_client, timeout_sec=30)
        cd_clean = get_default_collection(root_client, "congDan")
        cd_clean.delete_many({"maCongDan": {"$regex": f"^KB4_{variant_name}_"}})
        wait_for_replication_catchup(root_client, timeout_sec=20)

        st_init = root_client.admin.command("replSetGetStatus")
        term_before = st_init.get("term")
        primary_member = next(m for m in st_init["members"] if m["stateStr"] == "PRIMARY")
        old_primary = primary_member["name"].split(":")[0]
        sec_nodes = [m["name"].split(":")[0] for m in st_init["members"] if m["stateStr"] == "SECONDARY"]
        monitor_node = sec_nodes[0]

    stop_event = threading.Event()
    op_records: list[dict] = []
    base_dt = datetime(1996, 6, 1, tzinfo=timezone.utc)

    def writer_loop():
        with get_rs_client(
            use_root=False,
            retry_writes=retry_writes,
            server_selection_timeout_ms=15000,
        ) as w_client:
            cd = get_default_collection(w_client, "congDan")
            seq = 0
            while not stop_event.is_set():
                seq += 1
                ma_cd = f"KB4_{variant_name}_{seq:06d}"
                doc = {
                    "maCongDan": ma_cd,
                    "hoTen": f"Cong dan KB4 {variant_name} {seq:06d}",
                    "ngaySinh": base_dt,
                    "gioiTinh": "Nam" if seq % 2 else "Nu",
                    "trangThai": "thu_nghiem",
                    "lienHe": {"soDienThoai": "0944000000"},
                    "hoGiaDinhId": None,
                    "diaChi": {"thanhPho": "Vung_A", "quan": "Quan_1", "phuong": "Phuong_1", "chiTiet": "KB4"},
                    "phienBan": 1,
                    "updatedAt": datetime.now(timezone.utc),
                }
                rec = timed_operation(ma_cd, "insert_kb4", lambda d=doc: cd.insert_one(d))
                op_records.append(rec)
                time.sleep(0.02)

    writer_thread = threading.Thread(target=writer_loop, daemon=True)
    writer_thread.start()

    print(f"[Bước 1] Chạy tải ghi đánh số ổn định trong {pre_sec}s (Primary hiện tại: {old_primary})...")
    time.sleep(pre_sec)

    # Bước 2: Gây sự cố trên Primary
    print(f"[Bước 2] Thực hiện 'docker {fault_action} {old_primary}'...")
    fault_ev = inject_fault(fault_action, old_primary)
    t0_ns = fault_ev["perf_counter_ns"]

    # Bước 3: Lấy mẫu rs.status() từ một Secondary để phát hiện Primary mới (T2)
    t2_ns = None
    new_primary = None
    term_after = term_before
    with get_direct_client(monitor_node, use_root=True) as mon_client:
        deadline = time.time() + 45
        while time.time() < deadline:
            try:
                st = mon_client.admin.command("replSetGetStatus")
                p_list = [
                    m for m in st["members"]
                    if m.get("stateStr") == "PRIMARY" and not m["name"].startswith(old_primary)
                ]
                if p_list:
                    t2_ns = time.perf_counter_ns()
                    new_primary = p_list[0]["name"].split(":")[0]
                    term_after = st.get("term")
                    break
            except Exception:
                pass
            time.sleep(0.25)

    election_time_s = round((t2_ns - t0_ns) / 1e9, 2) if t2_ns else -1.0
    print(f"[Bước 3] Primary mới được bầu: {new_primary} (term={term_after}) | T2 - T0 = {election_time_s}s")

    print(f"[Bước 4] Duy trì tải ghi thêm {post_sec}s trên Primary mới...")
    time.sleep(post_sec)
    stop_event.set()
    writer_thread.join(timeout=10)

    save_operation_logs_csv(op_records, f"kb4_{variant_name}_operations.csv")

    # Bước 5: Tính T1, T3 và thống kê lỗi
    pre_ok = [r for r in op_records if r["status"] == "success" and r["t1_ns"] <= t0_ns]
    post_ok = [r for r in op_records if r["status"] == "success" and r["t1_ns"] > t0_ns]

    # Nếu thao tác có retryWrites thành công ngay sau khi bầu xong, tìm khoảng gián đoạn lớn nhất quanh sự cố
    all_ok = [r for r in op_records if r["status"] == "success"]
    max_gap_s = 0.0
    for idx in range(1, len(all_ok)):
        gap_s = (all_ok[idx]["t1_ns"] - all_ok[idx - 1]["t1_ns"]) / 1e9
        if gap_s > max_gap_s:
            max_gap_s = gap_s

    if pre_ok and post_ok:
        # T1 là thao tác thành công ngay trước các thao tác lỗi/chờ; T3 là thao tác thành công đầu tiên sau bầu chọn
        t3_t1_s = round(max_gap_s, 2)
    else:
        t3_t1_s = -1.0

    explicit_failures = sum(1 for r in op_records if r["status"] == "explicit_failure")
    uncertain_ops = sum(1 for r in op_records if r["status"] in ("write_concern_or_timeout", "no_response"))
    acknowledged_ids = {r["request_id"] for r in all_ok}

    # Bước 6: Kiểm tra mất dữ liệu trên Primary mới
    with get_direct_client(new_primary or monitor_node, use_root=True) as new_p_client:
        ndb = new_p_client[DB_NAME]
        stored_docs = ndb.congDan.find(
            {"maCongDan": {"$regex": f"^KB4_{variant_name}_"}},
            {"maCongDan": 1, "_id": 0},
        )
        stored_ids = {d["maCongDan"] for d in stored_docs}

    lost_acknowledged = len(acknowledged_ids - stored_ids)
    print(
        f"[Bước 5 & 6] T3 - T1 = {t3_t1_s}s | Lỗi rõ ràng: {explicit_failures} | "
        f"Không chắc chắn: {uncertain_ops} | Thao tác thành công bị mất: {lost_acknowledged}"
    )

    if restore_after:
        print(f"[Khôi phục] Khởi động lại {old_primary} để chuẩn bị lượt tiếp theo...")
        inject_fault("start", old_primary)
        with get_rs_client(use_root=True) as r_client:
            wait_for_replication_catchup(r_client, timeout_sec=60)
            get_default_collection(r_client, "congDan").delete_many(
                {"maCongDan": {"$regex": f"^KB4_{variant_name}_"}}
            )
            wait_for_replication_catchup(r_client, timeout_sec=30)

    return {
        "variant": variant_name,
        "old_primary": old_primary,
        "new_primary": new_primary,
        "term_before": term_before,
        "term_after": term_after,
        "t2_minus_t0_s": election_time_s,
        "t3_minus_t1_s": t3_t1_s,
        "explicit_failures": explicit_failures,
        "uncertain_operations": uncertain_ops,
        "acknowledged_count": len(acknowledged_ids),
        "lost_acknowledged_count": lost_acknowledged,
    }


def run_kb4(pre_sec: int = 8, post_sec: int = 12, leave_primary_stopped_at_end: bool = False) -> dict:
    print("======================================================================")
    print("KỊCH BẢN 4: KIỂM TRA FAILOVER KHI PRIMARY DỪNG HOẠT ĐỘNG (Mục 11.4)")
    print("======================================================================")

    v1 = run_single_failover_trial(
        "kill_retry", fault_action="kill", retry_writes=True, pre_sec=pre_sec, post_sec=post_sec, restore_after=True
    )
    v2 = run_single_failover_trial(
        "kill_no_retry", fault_action="kill", retry_writes=False, pre_sec=pre_sec, post_sec=post_sec, restore_after=True
    )
    v3 = run_single_failover_trial(
        "stop_retry",
        fault_action="stop",
        retry_writes=True,
        pre_sec=pre_sec,
        post_sec=post_sec,
        restore_after=not leave_primary_stopped_at_end,
    )

    summary = {
        "scenario": "KB4_Failover",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "variants": {
            "docker_kill_retry": v1,
            "docker_kill_no_retry": v2,
            "docker_stop_retry": v3,
        },
    }

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    with open(results_dir / "kb4_results.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str, ensure_ascii=False)

    md_content = f"""# KẾT QUẢ KỊCH BẢN 4 – KIỂM TRA FAILOVER (Bảng 11.9)

| Chỉ số | `docker kill`, có retry | `docker kill`, không retry | `docker stop`, có retry | Ghi chú |
| :--- | :---: | :---: | :---: | :--- |
| Node được bầu làm Primary mới | `{v1['new_primary']}` | `{v2['new_primary']}` | `{v3['new_primary']}` | Tăng `term` thêm 1 |
| `T2 - T0` (Thời gian bầu chọn, s) | `{v1['t2_minus_t0_s']} s` | `{v2['t2_minus_t0_s']} s` | `{v3['t2_minus_t0_s']} s` | Thời gian cụm bầu Primary mới |
| `T3 - T1` (Gián đoạn ghi ứng dụng, s) | `{v1['t3_minus_t1_s']} s` | `{v2['t3_minus_t1_s']} s` | `{v3['t3_minus_t1_s']} s` | Khoảng cách 2 thao tác thành công |
| Thao tác lỗi rõ ràng | `{v1['explicit_failures']}` | `{v2['explicit_failures']}` | `{v3['explicit_failures']}` | Bị từ chối trong lúc chuyển đổi |
| Thao tác không chắc chắn | `{v1['uncertain_operations']}` | `{v2['uncertain_operations']}` | `{v3['uncertain_operations']}` | Lỗi write concern / timeout / mất kết nối |
| Thao tác thành công bị mất | **`{v1['lost_acknowledged_count']}`** | **`{v2['lost_acknowledged_count']}`** | **`{v3['lost_acknowledged_count']}`** | Kỳ vọng `0` (Write Concern `majority`) |
"""
    with open(results_dir / "kb4_results.md", "w", encoding="utf-8") as f:
        f.write(md_content)

    print("[OK] Hoàn tất Kịch bản 4! Đã lưu kết quả tại: results/kb4_results.md")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pre-sec", type=int, default=8)
    parser.add_argument("--post-sec", type=int, default=12)
    args = parser.parse_args()
    run_kb4(pre_sec=args.pre_sec, post_sec=args.post_sec)
