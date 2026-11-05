#!/usr/bin/env python3
"""
Công cụ hỗ trợ giả lập sự cố trên máy chủ Docker theo Bảng 11.1 (Chương 11):
- stop:   docker stop <node> (Dừng có kiểm soát - SIGTERM, mô phỏng bảo trì)
- kill:   docker kill <node> (Dừng đột ngột - SIGKILL, mô phỏng sập tiến trình/mất điện)
- start:  docker start <node> (Khởi động lại node phục vụ kịch bản Recovery)
- isolate:   docker network disconnect cluster_net <node> (Cô lập mạng - mô phỏng phân vùng mạng / split-brain)
- reconnect: docker network connect cluster_net <node> (Khôi phục kết nối mạng cho node)

Mọi thao tác đều trả về mốc thời gian đơn điệu (perf_counter_ns) và thời gian thực UTC
để đối chiếu chính xác với log client và log mongod (Mục 11 - Quy ước đo thời gian).
"""

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NETWORK_NAME = "cluster_net"


def inject_fault(action: str, node: str, network: str = NETWORK_NAME) -> dict:
    if action == "stop":
        cmd = ["docker", "stop", node]
    elif action == "kill":
        cmd = ["docker", "kill", node]
    elif action == "start":
        cmd = ["docker", "start", node]
    elif action == "isolate":
        cmd = ["docker", "network", "disconnect", network, node]
    elif action == "reconnect":
        cmd = ["docker", "network", "connect", network, node]
    else:
        raise ValueError(f"Hành động không hợp lệ: {action}")

    t0_ns = time.perf_counter_ns()
    t0_utc = datetime.now(timezone.utc).isoformat()
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    t1_ns = time.perf_counter_ns()

    event = {
        "action": action,
        "node": node,
        "command": " ".join(cmd),
        "timestamp_utc": t0_utc,
        "perf_counter_ns": t0_ns,
        "duration_ms": round((t1_ns - t0_ns) / 1e6, 2),
        "returncode": res.returncode,
        "stdout": res.stdout.strip(),
        "stderr": res.stderr.strip(),
    }

    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    with open(logs_dir / "fault_events.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")

    return event


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Giả lập sự cố node MongoDB theo Bảng 11.1")
    parser.add_argument(
        "action",
        choices=["stop", "kill", "start", "isolate", "reconnect"],
        help="Loại tác động: stop | kill | start | isolate | reconnect",
    )
    parser.add_argument(
        "node",
        choices=["mongo1", "mongo2", "mongo3"],
        help="Tên container node mục tiêu",
    )
    args = parser.parse_args()
    ev = inject_fault(args.action, args.node)
    print(json.dumps(ev, indent=2, ensure_ascii=False))
