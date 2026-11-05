#!/usr/bin/env python3
"""
Script khởi tạo và cấu hình MongoDB Replica Set rs0 ba node (Mục 9.6, 10.3, 10.4):
- Khởi tạo Replica Set rs0 với:
    + mongo1:27017 (_id: 0, votes: 1, priority: 2)
    + mongo2:27017 (_id: 1, votes: 1, priority: 1)
    + mongo3:27017 (_id: 2, votes: 1, priority: 1)
- Thiết lập thông số nền:
    + heartbeatIntervalMillis: 2000
    + electionTimeoutMillis: 10000
    + writeConcernMajorityJournalDefault: true
- Chờ quá trình bầu chọn hoàn tất (1 PRIMARY, 2 SECONDARY).
- Tạo tài khoản thực nghiệm (lab_user) trên database cluster_lab.
"""

import os
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

REPLICA_SET = os.environ.get("REPLICA_SET_NAME", "rs0")
DB_NAME = os.environ.get("MONGO_DB_NAME", "cluster_lab")
ROOT_USER = os.environ.get("MONGO_INITDB_ROOT_USERNAME", "cluster_admin")
ROOT_PASS = os.environ.get("MONGO_INITDB_ROOT_PASSWORD", "ClusterAdminSecret2026!")
APP_USER = os.environ.get("APP_DB_USERNAME", "lab_user")
APP_PASS = os.environ.get("APP_DB_PASSWORD", "LabUserSecret2026!")

IN_DOCKER = os.environ.get("RUNNING_IN_DOCKER", "").lower() == "true"


def get_primary_seed_host() -> str:
    if IN_DOCKER:
        return "mongo1:27017"
    return "localhost:27017"


def wait_for_node(host: str, timeout_sec: int = 60) -> bool:
    uri = f"mongodb://{quote_plus(ROOT_USER)}:{quote_plus(ROOT_PASS)}@{host}/admin?authSource=admin&directConnection=true"
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
                client.admin.command("ping")
                return True
        except Exception:
            time.sleep(2)
    return False


def initiate_replica_set() -> None:
    seed_host = get_primary_seed_host()
    print(f"[1/4] Đang chờ tiến trình mongod tại {seed_host} sẵn sàng...")
    if not wait_for_node(seed_host, timeout_sec=90):
        print(f"[ERROR] Không thể kết nối tới {seed_host}. Hãy kiểm tra các container mongo1, mongo2, mongo3.")
        sys.exit(1)

    uri = f"mongodb://{quote_plus(ROOT_USER)}:{quote_plus(ROOT_PASS)}@{seed_host}/admin?authSource=admin&directConnection=true"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)

    rs_config = {
        "_id": REPLICA_SET,
        "writeConcernMajorityJournalDefault": True,
        "members": [
            {"_id": 0, "host": "mongo1:27017", "votes": 1, "priority": 2},
            {"_id": 1, "host": "mongo2:27017", "votes": 1, "priority": 1},
            {"_id": 2, "host": "mongo3:27017", "votes": 1, "priority": 1},
        ],
        "settings": {
            "heartbeatIntervalMillis": 2000,
            "electionTimeoutMillis": 10000,
            "chainingAllowed": True,
        },
    }

    print(f"[2/4] Kiểm tra và khởi tạo Replica Set '{REPLICA_SET}' (Mục 10.3 & 10.4)...")
    try:
        status = client.admin.command("replSetGetStatus")
        print(f"[INFO] Replica Set '{status.get('set')}' đã được khởi tạo từ trước (giữ nguyên cấu hình trong volume).")
    except OperationFailure as exc:
        # Code 94 = NotYetInitialized
        if exc.code == 94 or "no replset config" in str(exc).lower():
            res = client.admin.command("replSetInitiate", rs_config)
            print(f"[OK] Đã thực thi rs.initiate(): {res}")
        else:
            raise

    print("[3/4] Đang chờ quá trình bầu chọn (Election) hoàn tất (1 PRIMARY và 2 SECONDARY)...")
    deadline = time.time() + 60
    primary_host = None
    while time.time() < deadline:
        try:
            status = client.admin.command("replSetGetStatus")
            members = status.get("members", [])
            states = [m.get("stateStr") for m in members]
            primaries = [m.get("name") for m in members if m.get("stateStr") == "PRIMARY"]
            secondaries = [m.get("name") for m in members if m.get("stateStr") == "SECONDARY"]
            if len(primaries) == 1 and len(secondaries) == 2:
                primary_host = primaries[0]
                print(f"[OK] Cụm ổn định! PRIMARY: {primary_host} | SECONDARY: {', '.join(secondaries)}")
                break
            print(f"  ... Trạng thái hiện tại: {states}")
        except PyMongoError as e:
            print(f"  ... Đang chờ cụm ổn định ({e})")
        time.sleep(2)
    else:
        print("[WARN] Quá thời gian chờ 60s nhưng chưa đủ 1 PRIMARY & 2 SECONDARY. Hãy kiểm tra bằng verify_cluster.py.")

    client.close()

    # Kết nối tới Primary để tạo tài khoản thực nghiệm lab_user
    print(f"[4/4] Đang cấu hình tài khoản thực nghiệm '{APP_USER}' trên database '{DB_NAME}'...")
    hosts = "mongo1:27017,mongo2:27017,mongo3:27017" if IN_DOCKER else "localhost:27017"
    conn_opts = f"replicaSet={REPLICA_SET}" if IN_DOCKER else "directConnection=true"
    rs_uri = f"mongodb://{quote_plus(ROOT_USER)}:{quote_plus(ROOT_PASS)}@{hosts}/admin?authSource=admin&{conn_opts}"

    with MongoClient(rs_uri, serverSelectionTimeoutMS=15000) as rs_client:
        db = rs_client[DB_NAME]
        existing = db.command("usersInfo", APP_USER).get("users", [])
        roles = [
            {"role": "readWrite", "db": DB_NAME},
            {"role": "dbAdmin", "db": DB_NAME},
            {"role": "clusterMonitor", "db": "admin"},
            {"role": "read", "db": "local"},
        ]
        if not existing:
            db.command("createUser", APP_USER, pwd=APP_PASS, roles=roles)
            print(f"[OK] Đã tạo tài khoản thực nghiệm '{APP_USER}' với quyền readWrite trên '{DB_NAME}' & clusterMonitor.")
        else:
            db.command("updateUser", APP_USER, pwd=APP_PASS, roles=roles)
            print(f"[OK] Đã cập nhật tài khoản thực nghiệm '{APP_USER}'.")


if __name__ == "__main__":
    initiate_replica_set()
