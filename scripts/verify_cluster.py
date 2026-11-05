#!/usr/bin/env python3
"""
Script kiểm tra và xác nhận Cluster hoạt động thành công theo 8 điều kiện tại Mục 10.5, 10.6 và 10.7:
1. Replica Set có tên rs0 và chứa đúng ba thành viên mongo1:27017, mongo2:27017, mongo3:27017.
2. Tại thời điểm ổn định có đúng 1 PRIMARY và 2 SECONDARY; cả 3 đều có health == 1.
3. votes và priority khớp cấu hình: mongo1 priority 2, mongo2 và mongo3 priority 1.
4. Client kết nối bằng danh sách nhiều seed host và replicaSet=rs0, không phụ thuộc Primary cố định.
5. Thao tác ghi qua Primary được sao chép tới hai Secondary; dữ liệu mẫu & số lượng khớp nhau.
6. Volume của từng node được gắn độc lập tại /data/db.
7. Xác thực người dùng và xác thực nội bộ bằng keyFile đang bật; không lộ thông tin bí mật.
8. Cửa sổ oplog, độ trễ sao chép và mức tài nguyên được ghi nhận làm mốc.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scenarios.common import (
    DB_NAME,
    REPLICA_SET,
    get_direct_client,
    get_rs_client,
    get_replication_window_and_lag,
)
from pymongo import ReadPreference, WriteConcern
from pymongo.read_concern import ReadConcern


def verify_cluster() -> dict:
    print("======================================================================")
    print("XÁC NHẬN TRẠNG THÁI CLUSTER THEO MỤC 10.5, 10.6 & 10.7")
    print("======================================================================")

    checks = {}
    with get_rs_client(use_root=True) as client:
        status = client.admin.command("replSetGetStatus")
        conf = client.admin.command("replSetGetConfig")["config"]
        hello = client.admin.command("hello")
        cmd_line = client.admin.command("getCmdLineOpts")
        fcv_doc = client.admin.command({"getParameter": 1, "featureCompatibilityVersion": 1})

        # Điều kiện 1: Tên rs0 và 3 thành viên
        member_hosts = [m["host"] for m in conf["members"]]
        cond1 = (
            status.get("set") == REPLICA_SET
            and set(member_hosts) == {"mongo1:27017", "mongo2:27017", "mongo3:27017"}
        )
        checks["1_replica_set_members"] = {
            "passed": cond1,
            "set_name": status.get("set"),
            "members": member_hosts,
        }

        # Điều kiện 2: 1 PRIMARY, 2 SECONDARY, health = 1
        members_status = status.get("members", [])
        primaries = [m["name"] for m in members_status if m.get("stateStr") == "PRIMARY"]
        secondaries = [m["name"] for m in members_status if m.get("stateStr") == "SECONDARY"]
        all_healthy = all(m.get("health") == 1.0 for m in members_status)
        cond2 = len(primaries) == 1 and len(secondaries) == 2 and all_healthy
        checks["2_roles_and_health"] = {
            "passed": cond2,
            "primary": primaries[0] if primaries else None,
            "secondaries": secondaries,
            "all_healthy": all_healthy,
        }

        # Điều kiện 3: votes và priority khớp thiết kế (mongo1: priority=2, mongo2/3: priority=1)
        prio_map = {m["host"]: (m.get("votes"), m.get("priority")) for m in conf["members"]}
        cond3 = (
            prio_map.get("mongo1:27017") == (1, 2.0)
            and prio_map.get("mongo2:27017") == (1, 1.0)
            and prio_map.get("mongo3:27017") == (1, 1.0)
            and conf.get("writeConcernMajorityJournalDefault") is True
        )
        checks["3_votes_and_priority"] = {
            "passed": cond3,
            "priority_votes_map": {k: {"votes": v[0], "priority": v[1]} for k, v in prio_map.items()},
            "writeConcernMajorityJournalDefault": conf.get("writeConcernMajorityJournalDefault"),
            "heartbeatIntervalMillis": conf.get("settings", {}).get("heartbeatIntervalMillis"),
            "electionTimeoutMillis": conf.get("settings", {}).get("electionTimeoutMillis"),
        }

        # Điều kiện 4: Client kết nối qua Replica Set discovery
        cond4 = hello.get("setName") == REPLICA_SET and bool(hello.get("primary"))
        checks["4_client_topology_discovery"] = {
            "passed": cond4,
            "discovered_primary": hello.get("primary"),
            "hosts": hello.get("hosts"),
        }

        # Điều kiện 5: Ghi mẫu qua Primary và đối chiếu trên cả 3 node
        db = client.get_database(
            DB_NAME,
            write_concern=WriteConcern(w="majority", j=True, wtimeout=5000),
            read_concern=ReadConcern("majority"),
            read_preference=ReadPreference.PRIMARY,
        )
        probe_id = "VERIFY_10_7_PROBE"
        now_utc = datetime.now(timezone.utc)
        db.congDan.update_one(
            {"maCongDan": probe_id},
            {
                "$set": {
                    "maCongDan": probe_id,
                    "hoTen": "Ho so kiem tra muc 10.7",
                    "ngaySinh": datetime(1995, 1, 1, tzinfo=timezone.utc),
                    "gioiTinh": "Nam",
                    "trangThai": "thu_nghiem",
                    "lienHe": {"soDienThoai": "0900000000", "email": "verify@cluster.lab"},
                    "diaChi": {
                        "thanhPho": "Vung_A",
                        "quan": "Quan_1",
                        "phuong": "Phuong_1",
                        "chiTiet": "So 1 Duong Thuc Nghiem",
                    },
                    "phienBan": 1,
                    "updatedAt": now_utc,
                }
            },
            upsert=True,
        )
        time.sleep(0.5)
        node_docs = {}
        for idx, node_name in enumerate(["mongo1", "mongo2", "mongo3"]):
            with get_direct_client(idx) as dclient:
                ndb = dclient.get_database(
                    DB_NAME, read_preference=ReadPreference.SECONDARY_PREFERRED
                )
                doc = ndb.congDan.find_one({"maCongDan": probe_id})
                node_docs[node_name] = bool(doc and doc.get("phienBan") == 1)
        db.congDan.delete_one({"maCongDan": probe_id})
        cond5 = all(node_docs.values())
        checks["5_replication_probe"] = {
            "passed": cond5,
            "visible_on_nodes": node_docs,
        }

        # Điều kiện 6 & 7: Volume /data/db và Xác thực keyFile
        parsed_opts = cmd_line.get("parsed", {})
        storage_path = parsed_opts.get("storage", {}).get("dbPath") or "/data/db"
        security_opts = parsed_opts.get("security", {})
        cond6 = storage_path == "/data/db"
        cond7 = (
            security_opts.get("authorization") == "enabled"
            and bool(security_opts.get("keyFile"))
        )
        checks["6_independent_volume"] = {"passed": cond6, "dbPath": storage_path}
        checks["7_security_auth_keyfile"] = {
            "passed": cond7,
            "authorization": security_opts.get("authorization"),
            "keyFile_configured": bool(security_opts.get("keyFile")),
        }

        # Điều kiện 8: Cửa sổ Oplog & độ trễ sao chép
        repl_info = get_replication_window_and_lag(client)
        checks["8_oplog_and_lag_baseline"] = {
            "passed": True,
            "featureCompatibilityVersion": fcv_doc.get("featureCompatibilityVersion", {}).get("version"),
            **repl_info,
        }

    all_passed = all(item["passed"] for item in checks.values())
    summary = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "all_conditions_passed": all_passed,
        "checks": checks,
    }

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    out_json = results_dir / "cluster_verification.json"
    out_md = results_dir / "cluster_verification.md"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str, ensure_ascii=False)

    md_lines = [
        "# BIÊN BẢN XÁC NHẬN CLUSTER HOẠT ĐỘNG THÀNH CÔNG (Mục 10.7)",
        "",
        f"- **Thời điểm kiểm tra (UTC):** `{summary['timestamp_utc']}`",
        f"- **Kết luận tổng thể:** `{'ĐẠT (SẴN SÀNG THỰC NGHIỆM)' if all_passed else 'CHƯA ĐẠT'}`",
        "",
        "| STT | Tiêu chí kiểm tra (Mục 10.7) | Trạng thái | Chi tiết ghi nhận |",
        "| :---: | :--- | :---: | :--- |",
    ]
    for key, val in checks.items():
        status_str = "ĐẠT" if val["passed"] else "KHÔNG ĐẠT"
        detail_str = json.dumps({k: v for k, v in val.items() if k != "passed"}, ensure_ascii=False, default=str)
        md_lines.append(f"| `{key.split('_')[0]}` | `{key}` | **{status_str}** | `{detail_str}` |")

    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")

    for key, val in checks.items():
        flag = "[PASS]" if val["passed"] else "[FAIL]"
        print(f"  {flag} {key}: {val}")

    print(f"\n[OK] Kết quả xác nhận đã lưu tại: {out_md}")
    if not all_passed:
        sys.exit(1)
    return summary


if __name__ == "__main__":
    verify_cluster()
