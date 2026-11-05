#!/usr/bin/env python3
"""
Module dùng chung cho các kịch bản thực nghiệm Cluster (Chương 9, 10, 11):
- Quản lý kết nối Replica Set (rs0), kết nối trực tiếp từng node, và kết nối Standalone.
- Phân loại kết quả thao tác theo 4 nhóm chuẩn của Chương 11:
  1. success: Đã nhận xác nhận theo write concern
  2. explicit_failure: Lỗi trả về trước khi gửi hoặc bị từ chối rõ ràng
  3. write_concern_or_timeout: Lỗi write concern hoặc timeout (không chắc chắn đã ghi hay chưa)
  4. no_response: Mất kết nối/không nhận được phản hồi sau khi gửi
- Đo thời gian bằng đồng hồ đơn điệu time.perf_counter_ns() kèm mốc UTC.
- Tính toán phân vị độ trễ (trung bình, p50, p95, p99, max).
"""

import csv
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
from typing import Any, Callable
from urllib.parse import quote_plus

from dotenv import load_dotenv
from pymongo import MongoClient, ReadPreference, WriteConcern
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    ExecutionTimeout,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    ServerSelectionTimeoutError,
    WTimeoutError,
    WriteConcernError,
    WriteError,
)
from pymongo.read_concern import ReadConcern

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

REPLICA_SET = os.environ.get("REPLICA_SET_NAME", "rs0")
DB_NAME = os.environ.get("MONGO_DB_NAME", "cluster_lab")
ROOT_USER = os.environ.get("MONGO_INITDB_ROOT_USERNAME", "cluster_admin")
ROOT_PASS = os.environ.get("MONGO_INITDB_ROOT_PASSWORD", "ClusterAdminSecret2026!")
APP_USER = os.environ.get("APP_DB_USERNAME", "lab_user")
APP_PASS = os.environ.get("APP_DB_PASSWORD", "LabUserSecret2026!")
SEED_VALUE = int(os.environ.get("SEED_VALUE", "42"))

IN_DOCKER = os.environ.get("RUNNING_IN_DOCKER", "").lower() == "true"

INTERNAL_NODES = ["mongo1:27017", "mongo2:27017", "mongo3:27017"]
EXTERNAL_NODES = ["localhost:27017", "localhost:27018", "localhost:27019"]
NODE_NAMES = ["mongo1", "mongo2", "mongo3"]


def get_rs_uri(
    use_root: bool = False,
    retry_writes: bool = True,
    retry_reads: bool = True,
    server_selection_timeout_ms: int = 30000,
) -> str:
    user = ROOT_USER if use_root else APP_USER
    pwd = ROOT_PASS if use_root else APP_PASS
    auth_db = "admin" if use_root else DB_NAME
    hosts = ",".join(INTERNAL_NODES if IN_DOCKER else EXTERNAL_NODES)
    rw_str = "true" if retry_writes else "false"
    rr_str = "true" if retry_reads else "false"
    return (
        f"mongodb://{quote_plus(user)}:{quote_plus(pwd)}@{hosts}/{DB_NAME}"
        f"?authSource={auth_db}&replicaSet={REPLICA_SET}"
        f"&retryWrites={rw_str}&retryReads={rr_str}"
        f"&serverSelectionTimeoutMS={server_selection_timeout_ms}"
    )


def get_rs_client(
    use_root: bool = False,
    retry_writes: bool = True,
    retry_reads: bool = True,
    server_selection_timeout_ms: int = 30000,
) -> MongoClient:
    uri = get_rs_uri(
        use_root=use_root,
        retry_writes=retry_writes,
        retry_reads=retry_reads,
        server_selection_timeout_ms=server_selection_timeout_ms,
    )
    return MongoClient(uri)


def get_direct_client(
    node: int | str,
    use_root: bool = False,
    timeout_ms: int = 5000,
) -> MongoClient:
    """
    Kết nối trực tiếp tới một node (directConnection=true).
    Mục 9.6 & 11: Chỉ dùng khi cần đọc riêng từng bản sao để đối chiếu hoặc kiểm tra trạng thái.
    """
    if isinstance(node, int):
        idx = node
    else:
        node_clean = node.split(":")[0].strip()
        idx = NODE_NAMES.index(node_clean) if node_clean in NODE_NAMES else 0

    host = INTERNAL_NODES[idx] if IN_DOCKER else EXTERNAL_NODES[idx]
    user = ROOT_USER if use_root else APP_USER
    pwd = ROOT_PASS if use_root else APP_PASS
    auth_db = "admin" if use_root else DB_NAME
    uri = (
        f"mongodb://{quote_plus(user)}:{quote_plus(pwd)}@{host}/{DB_NAME}"
        f"?authSource={auth_db}&directConnection=true"
        f"&serverSelectionTimeoutMS={timeout_ms}&connectTimeoutMS={timeout_ms}"
    )
    return MongoClient(uri)


def get_standalone_client(use_root: bool = True, timeout_ms: int = 10000) -> MongoClient:
    host = (
        os.environ.get("MONGO_STANDALONE_INTERNAL", "mongo_standalone:27017")
        if IN_DOCKER
        else os.environ.get("MONGO_STANDALONE_EXTERNAL", "localhost:27020")
    )
    user = ROOT_USER if use_root else APP_USER
    pwd = ROOT_PASS if use_root else APP_PASS
    auth_db = "admin" if use_root else DB_NAME
    uri = (
        f"mongodb://{quote_plus(user)}:{quote_plus(pwd)}@{host}/{DB_NAME}"
        f"?authSource={auth_db}&directConnection=true"
        f"&serverSelectionTimeoutMS={timeout_ms}"
    )
    return MongoClient(uri)


def get_default_collection(
    client: MongoClient,
    collection_name: str = "congDan",
    w: int | str = "majority",
    j: bool | None = True,
    wtimeout: int = 5000,
    read_concern_level: str = "majority",
    read_pref=ReadPreference.PRIMARY,
):
    wc_kwargs: dict[str, Any] = {"w": w, "wtimeout": wtimeout}
    if j is not None:
        wc_kwargs["j"] = j
    db = client.get_database(
        DB_NAME,
        write_concern=WriteConcern(**wc_kwargs),
        read_concern=ReadConcern(read_concern_level),
        read_preference=read_pref,
    )
    return db[collection_name]


def classify_operation_error(exc: Exception) -> str:
    """
    Phân loại lỗi theo đúng Quy ước đo thời gian tại Chương 11:
    - write_concern_or_timeout: Lỗi write concern hoặc timeout (không chắc chắn đã ghi hay chưa)
    - no_response: Lỗi mạng/mất kết nối trong lúc thực thi (không nhận được phản hồi)
    - explicit_failure: Lỗi trả về trước khi gửi hoặc bị từ chối rõ ràng
    """
    if isinstance(exc, (WTimeoutError, WriteConcernError, NetworkTimeout, ExecutionTimeout)):
        return "write_concern_or_timeout"
    if isinstance(exc, (NotPrimaryError, ServerSelectionTimeoutError, WriteError)):
        return "explicit_failure"
    if isinstance(exc, (AutoReconnect, ConnectionFailure)):
        return "no_response"
    if isinstance(exc, OperationFailure):
        # Các mã lỗi liên quan writeConcern hoặc timeout
        if exc.code in (50, 64, 79, 100):
            return "write_concern_or_timeout"
        return "explicit_failure"
    return "explicit_failure"


def timed_operation(request_id: str, op_type: str, fn: Callable[[], Any]) -> dict:
    """
    Thực thi một thao tác có gắn mã yêu cầu duy nhất và đo bằng đồng hồ đơn điệu
    time.perf_counter_ns() kèm mốc thời gian thực UTC (Chương 11).
    """
    utc_start = datetime.now(timezone.utc).isoformat()
    t0_ns = time.perf_counter_ns()
    status = "success"
    error_msg = ""
    result = None

    try:
        result = fn()
    except Exception as exc:
        status = classify_operation_error(exc)
        error_msg = f"{type(exc).__name__}: {exc}"

    t1_ns = time.perf_counter_ns()
    utc_end = datetime.now(timezone.utc).isoformat()
    latency_ms = round((t1_ns - t0_ns) / 1e6, 3)

    return {
        "request_id": request_id,
        "op_type": op_type,
        "utc_start": utc_start,
        "utc_end": utc_end,
        "t0_ns": t0_ns,
        "t1_ns": t1_ns,
        "latency_ms": latency_ms,
        "status": status,
        "error": error_msg,
        "result": result,
    }


def compute_latency_stats(latencies_ms: list[float]) -> dict:
    if not latencies_ms:
        return {
            "count": 0,
            "mean_ms": 0.0,
            "min_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "max_ms": 0.0,
        }
    arr = sorted(latencies_ms)
    n = len(arr)

    def percentile(p: float) -> float:
        idx = min(n - 1, max(0, math.ceil((p / 100.0) * n) - 1))
        return round(arr[idx], 3)

    return {
        "count": n,
        "mean_ms": round(sum(arr) / n, 3),
        "min_ms": round(arr[0], 3),
        "p50_ms": percentile(50),
        "p95_ms": percentile(95),
        "p99_ms": percentile(99),
        "max_ms": round(arr[-1], 3),
    }


def get_replication_window_and_lag(client: MongoClient | None = None) -> dict:
    """
    Thu thập cửa sổ oplog (tương đương rs.printReplicationInfo())
    và độ trễ sao chép của từng Secondary (tương đương rs.printSecondaryReplicationInfo()).
    """
    close_after = False
    if client is None:
        client = get_rs_client(use_root=True)
        close_after = True

    try:
        status = client.admin.command("replSetGetStatus")
        members = status.get("members", [])
        primary = next((m for m in members if m.get("stateStr") == "PRIMARY"), None)
        secondaries = [m for m in members if m.get("stateStr") == "SECONDARY"]

        primary_optime = primary.get("optimeDate") if primary else None
        lag_by_node = {}
        max_lag_sec = 0.0
        for sec in secondaries:
            sec_optime = sec.get("optimeDate")
            if primary_optime and sec_optime:
                lag_s = max(0.0, (primary_optime - sec_optime).total_seconds())
            else:
                lag_s = -1.0
            lag_by_node[sec["name"]] = round(lag_s, 3)
            if lag_s > max_lag_sec:
                max_lag_sec = round(lag_s, 3)

        # Cửa sổ oplog từ local.oplog.rs
        local_db = client["local"]
        oplog_stats = local_db.command("collStats", "oplog.rs")
        first_doc = local_db["oplog.rs"].find({}, {"ts": 1}).sort([("$natural", 1)]).limit(1)
        last_doc = local_db["oplog.rs"].find({}, {"ts": 1}).sort([("$natural", -1)]).limit(1)
        first_list = list(first_doc)
        last_list = list(last_doc)
        window_sec = 0
        if first_list and last_list:
            window_sec = last_list[0]["ts"].time - first_list[0]["ts"].time

        return {
            "primary": primary["name"] if primary else None,
            "oplog_configured_max_mb": round(oplog_stats.get("maxSize", 0) / (1024 * 1024), 2),
            "oplog_used_mb": round(oplog_stats.get("size", 0) / (1024 * 1024), 2),
            "oplog_window_seconds": window_sec,
            "oplog_window_hours": round(window_sec / 3600.0, 2),
            "secondary_lag_seconds": lag_by_node,
            "max_secondary_lag_seconds": max_lag_sec,
        }
    finally:
        if close_after:
            client.close()


def wait_for_replication_catchup(client: MongoClient, timeout_sec: int = 60) -> bool:
    """Chờ cả 2 Secondary đạt trạng thái SECONDARY và có optimeDate khớp với Primary."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            status = client.admin.command("replSetGetStatus")
            members = status.get("members", [])
            primaries = [m for m in members if m.get("stateStr") == "PRIMARY"]
            secondaries = [m for m in members if m.get("stateStr") == "SECONDARY" and m.get("health") == 1.0]
            if len(primaries) == 1 and len(secondaries) == 2:
                p_optime = primaries[0].get("optimeDate")
                if all(abs((p_optime - s.get("optimeDate", p_optime)).total_seconds()) == 0 for s in secondaries):
                    return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def get_node_db_snapshot(node_idx: int) -> dict:
    """Đọc trực tiếp từ một node: số lượng document, số lượng index và dbHash của từng collection."""
    with get_direct_client(node_idx, use_root=True) as dclient:
        db = dclient.get_database(DB_NAME, read_preference=ReadPreference.SECONDARY_PREFERRED)
        hash_res = db.command({"dbHash": 1})
        collections_hash = hash_res.get("collections", {})
        counts = {}
        indexes = {}
        for cname in ["congDan", "hoGiaDinh", "cuTru", "bienDong"]:
            counts[cname] = db[cname].count_documents({})
            indexes[cname] = len(list(db[cname].list_indexes()))
        return {
            "node": NODE_NAMES[node_idx],
            "counts": counts,
            "index_counts": indexes,
            "dbHash_collections": collections_hash,
            "dbHash_md5": hash_res.get("md5"),
        }


def save_operation_logs_csv(records: list[dict], filename: str) -> Path:
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_path = logs_dir / filename
    fieldnames = [
        "request_id",
        "op_type",
        "utc_start",
        "utc_end",
        "t0_ns",
        "t1_ns",
        "latency_ms",
        "status",
        "error",
    ]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    return out_path
