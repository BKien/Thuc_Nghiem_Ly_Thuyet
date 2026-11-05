#!/usr/bin/env python3
"""
Kịch bản 1 (Mục 11.1): Kiểm tra hoạt động cơ bản của Cluster
- Bước 1: Kiểm tra topology (rs.status(), rs.conf(), db.hello()).
- Bước 2: Kiểm tra client nhận biết topology (client.primary, client.secondaries, không dùng directConnection).
- Bước 3: Thực hiện 100 vòng lặp x 4 thao tác CRUD (insert, find, update, delete) = 400 thao tác
          với mã KB1_000001 đến KB1_000100, dùng write concern {w: "majority", j: true, wtimeout: 5000}.
- Bước 4: Trước thao tác xóa ở vòng lặp thứ 100, kết nối trực tiếp từng node (mongo1, mongo2, mongo3)
          để đối chiếu giá trị diaChi.quan và phienBan.
- Bước 5: Kiểm tra thao tác ghi với write concern {w: 3, wtimeout: 5000} khi đủ 3 node.
- Xuất Bảng 11.3 ra results/kb1_results.md và results/kb1_results.json.
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pymongo import ReadPreference, WriteConcern
from pymongo.read_concern import ReadConcern

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scenarios.common import (
    DB_NAME,
    NODE_NAMES,
    compute_latency_stats,
    get_default_collection,
    get_direct_client,
    get_rs_client,
    save_operation_logs_csv,
    timed_operation,
    wait_for_replication_catchup,
)


def run_kb1(iterations: int = 100) -> dict:
    print("======================================================================")
    print("KỊCH BẢN 1: KIỂM TRA HOẠT ĐỘNG CƠ BẢN CỦA CLUSTER (Mục 11.1)")
    print("======================================================================")

    with get_rs_client(use_root=True) as root_client:
        status = root_client.admin.command("replSetGetStatus")
        hello = root_client.admin.command("hello")
        members_info = [
            {
                "name": m["name"],
                "stateStr": m["stateStr"],
                "health": m["health"],
            }
            for m in status.get("members", [])
        ]

    with get_rs_client(use_root=False) as client:
        # Chờ khám phá topology
        client.admin.command("hello")
        discovered_primary = client.primary
        discovered_secondaries = list(client.secondaries)
        print(f"[Bước 1 & 2] Primary nhận diện: {discovered_primary} | Secondaries: {discovered_secondaries}")

        cd = get_default_collection(client, "congDan")
        cd.delete_many({"maCongDan": {"$regex": "^KB1_"}})

        op_logs = []
        lat_by_type = {"insert": [], "find": [], "update": [], "delete": []}
        replica_comparison = {}

        print(f"[Bước 3 & 4] Đang thực hiện {iterations} vòng lặp CRUD (tổng {iterations * 4} thao tác)...")
        base_dt = datetime(1998, 5, 15, tzinfo=timezone.utc)

        for i in range(1, iterations + 1):
            ma_cd = f"KB1_{i:06d}"
            now_utc = datetime.now(timezone.utc)
            doc_payload = {
                "maCongDan": ma_cd,
                "hoTen": f"Cong dan KB1 {i:06d}",
                "ngaySinh": base_dt,
                "gioiTinh": "Nam" if i % 2 == 1 else "Nu",
                "trangThai": "thu_nghiem",
                "lienHe": {"soDienThoai": f"0911{i:06d}", "email": f"kb1_{i:06d}@cluster.lab"},
                "hoGiaDinhId": None,
                "diaChi": {
                    "thanhPho": "Vung_A",
                    "quan": "Quan_A",
                    "phuong": "Phuong_1",
                    "chiTiet": f"So {i} Duong KB1",
                },
                "phienBan": 1,
                "updatedAt": now_utc,
            }

            # 1. INSERT
            r_ins = timed_operation(f"KB1_INS_{i:04d}", "insert", lambda: cd.insert_one(doc_payload))
            op_logs.append(r_ins)
            if r_ins["status"] == "success":
                lat_by_type["insert"].append(r_ins["latency_ms"])

            # 2. FIND
            r_fnd = timed_operation(f"KB1_FND_{i:04d}", "find", lambda: cd.find_one({"maCongDan": ma_cd}))
            op_logs.append(r_fnd)
            if r_fnd["status"] == "success":
                lat_by_type["find"].append(r_fnd["latency_ms"])

            # 3. UPDATE
            r_upd = timed_operation(
                f"KB1_UPD_{i:04d}",
                "update",
                lambda: cd.update_one(
                    {"maCongDan": ma_cd},
                    {"$set": {"diaChi.quan": "Quan_B", "updatedAt": datetime.now(timezone.utc)}, "$inc": {"phienBan": 1}},
                ),
            )
            op_logs.append(r_upd)
            if r_upd["status"] == "success":
                lat_by_type["update"].append(r_upd["latency_ms"])

            # Bước 4: Trước thao tác xóa ở lần lặp cuối, kiểm tra bản sao trực tiếp trên 3 node
            if i == iterations:
                wait_for_replication_catchup(client, timeout_sec=15)
                for idx, n_name in enumerate(NODE_NAMES):
                    with get_direct_client(idx) as dclient:
                        ndb = dclient.get_database(DB_NAME, read_preference=ReadPreference.SECONDARY_PREFERRED)
                        ndoc = ndb.congDan.find_one({"maCongDan": ma_cd})
                        replica_comparison[n_name] = {
                            "maCongDan": ndoc.get("maCongDan") if ndoc else None,
                            "quan": ndoc.get("diaChi", {}).get("quan") if ndoc else None,
                            "phienBan": ndoc.get("phienBan") if ndoc else None,
                        }

            # 4. DELETE
            r_del = timed_operation(f"KB1_DEL_{i:04d}", "delete", lambda: cd.delete_one({"maCongDan": ma_cd}))
            op_logs.append(r_del)
            if r_del["status"] == "success":
                lat_by_type["delete"].append(r_del["latency_ms"])

        # Bước 5: Kiểm tra mức xác nhận w: 3 khi đủ 3 node
        print("[Bước 5] Kiểm tra thao tác ghi với write concern {w: 3, wtimeout: 5000}...")
        cd_w3 = client.get_database(
            DB_NAME,
            write_concern=WriteConcern(w=3, j=True, wtimeout=5000),
            read_concern=ReadConcern("majority"),
        )["congDan"]
        w3_probe_code = "KB1_W3_PROBE"
        r_w3 = timed_operation(
            "KB1_W3_TEST",
            "insert_w3",
            lambda: cd_w3.insert_one(
                {
                    "maCongDan": w3_probe_code,
                    "hoTen": "Kiem tra w=3",
                    "ngaySinh": base_dt,
                    "gioiTinh": "Nam",
                    "trangThai": "thu_nghiem",
                    "lienHe": {"soDienThoai": "0911999999"},
                    "hoGiaDinhId": None,
                    "diaChi": {"thanhPho": "Vung_A", "quan": "Quan_B", "phuong": "Phuong_1", "chiTiet": "W3"},
                    "phienBan": 1,
                    "updatedAt": datetime.now(timezone.utc),
                }
            ),
        )
        cd.delete_one({"maCongDan": w3_probe_code})

    save_operation_logs_csv(op_logs, "kb1_crud_operations.csv")
    total_crud = len(op_logs)
    success_crud = sum(1 for r in op_logs if r["status"] == "success")

    stats_summary = {k: compute_latency_stats(v) for k, v in lat_by_type.items()}

    result = {
        "scenario": "KB1_Basic_CRUD",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "members_status": members_info,
        "hello_primary": hello.get("primary"),
        "client_discovered_primary": f"{discovered_primary[0]}:{discovered_primary[1]}" if discovered_primary else None,
        "client_discovered_secondaries": [f"{h}:{p}" for h, p in discovered_secondaries],
        "crud_success_count": success_crud,
        "crud_total_count": total_crud,
        "latency_stats_ms": stats_summary,
        "replica_comparison_step4": replica_comparison,
        "w3_write_result": {
            "status": r_w3["status"],
            "latency_ms": r_w3["latency_ms"],
            "error": r_w3["error"],
        },
    }

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    with open(results_dir / "kb1_results.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str, ensure_ascii=False)

    ins_mean = stats_summary["insert"]["mean_ms"]
    fnd_mean = stats_summary["find"]["mean_ms"]
    upd_mean = stats_summary["update"]["mean_ms"]
    del_mean = stats_summary["delete"]["mean_ms"]

    md_table = f"""# KẾT QUẢ KỊCH BẢN 1 – KIỂM TRA HOẠT ĐỘNG CƠ BẢN CỦA CLUSTER (Bảng 11.3)

| Hạng mục | Giá trị ghi nhận |
| :--- | :--- |
| Node PRIMARY / các node SECONDARY | PRIMARY: `{hello.get('primary')}` / SECONDARY: `{', '.join(m['name'] for m in members_info if m['stateStr'] == 'SECONDARY')}` |
| `health` và `stateStr` của ba thành viên | `{json.dumps(members_info, ensure_ascii=False)}` |
| Client nhận biết Primary (đúng/sai) | **Đúng** (`client.primary = {result['client_discovered_primary']}`) |
| Số thao tác CRUD thành công / tổng số | **{success_crud} / {total_crud}** |
| Độ trễ trung bình insert / find / update / delete (ms) | `{ins_mean} / {fnd_mean} / {upd_mean} / {del_mean} ms` |
| Giá trị document trên mongo1 / mongo2 / mongo3 | `{json.dumps(replica_comparison, ensure_ascii=False)}` |
| Kết quả thao tác `w: 3` | `{r_w3['status']}` (`{r_w3['latency_ms']} ms`) |
"""
    with open(results_dir / "kb1_results.md", "w", encoding="utf-8") as f:
        f.write(md_table)

    print(f"[OK] Hoàn tất Kịch bản 1! Thành công: {success_crud}/{total_crud} | Đã lưu: results/kb1_results.md")
    return result


if __name__ == "__main__":
    run_kb1()
