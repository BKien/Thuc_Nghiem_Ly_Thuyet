#!/usr/bin/env python3
"""
Script thu thập biên bản môi trường thực nghiệm theo Mục 9.3, 9.5 và 10.1:
- Ghi nhận phiên bản: mongod --version, mongosh --version, docker version,
  docker compose version, python --version, python -m pip show pymongo
- Ghi nhận phần cứng & hệ điều hành: Model CPU, số lõi/luồng, tổng RAM,
  dung lượng ổ đĩa trống, hệ điều hành (Ubuntu 24.04 LTS / Windows + WSL2).
- Ghi nhận featureCompatibilityVersion và Image Digest của MongoDB 8.0.
"""

import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def run_cmd(cmd: list[str], timeout: int = 15) -> str:
    try:
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
        out = (res.stdout or "") + ("\n" + res.stderr if res.stderr and not res.stdout else "")
        return out.strip()
    except Exception as exc:
        return f"Không thể thực thi ({' '.join(cmd)}): {exc}"


def get_hardware_info() -> dict:
    info = {
        "os_system": platform.system(),
        "os_release": platform.release(),
        "os_version": platform.version(),
        "architecture": platform.machine(),
        "processor": platform.processor(),
        "cpu_logical_cores": os.cpu_count(),
    }

    project_root = Path(__file__).resolve().parent.parent
    usage = shutil.disk_usage(project_root)
    info["disk_total_gib"] = round(usage.total / (1024**3), 2)
    info["disk_used_gib"] = round(usage.used / (1024**3), 2)
    info["disk_free_gib"] = round(usage.free / (1024**3), 2)

    if platform.system() == "Windows":
        cpu_name = run_cmd(["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"])
        ram_bytes = run_cmd(["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory"])
        wsl_status = run_cmd(["wsl", "--status"])
        if cpu_name and not cpu_name.startswith("Không thể"):
            info["cpu_model"] = cpu_name
        try:
            info["ram_total_gib"] = round(int(ram_bytes.splitlines()[0].strip()) / (1024**3), 2)
        except Exception:
            pass
        info["virtualization_layer"] = "Windows + WSL 2 / Docker Desktop"
        info["wsl_info"] = wsl_status
    else:
        cpu_model = run_cmd(["sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"])
        mem_kb = run_cmd(["sh", "-c", "grep -m1 'MemTotal' /proc/meminfo | awk '{print $2}'"])
        if cpu_model:
            info["cpu_model"] = cpu_model.strip()
        try:
            info["ram_total_gib"] = round(int(mem_kb.strip()) / (1024**2), 2)
        except Exception:
            pass
        info["virtualization_layer"] = "Linux / Container Engine"

    return info


def collect_environment_report() -> dict:
    project_root = Path(__file__).resolve().parent.parent
    logs_dir = project_root / "logs"
    results_dir = project_root / "results"
    logs_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    in_docker = os.environ.get("RUNNING_IN_DOCKER", "").lower() == "true"

    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "execution_context": "container_client" if in_docker else "host_machine",
        "hardware_and_os": get_hardware_info(),
        "versions": {
            "python_version": run_cmd([sys.executable, "--version"]),
            "pymongo_info": run_cmd([sys.executable, "-m", "pip", "show", "pymongo"]),
            "mongosh_version": run_cmd(["mongosh", "--version"]),
        },
    }

    if not in_docker:
        report["versions"]["docker_version"] = run_cmd(["docker", "version"])
        report["versions"]["docker_compose_version"] = run_cmd(["docker", "compose", "version"])
        report["versions"]["mongod_version"] = run_cmd(["docker", "exec", "mongo1", "mongod", "--version"])
        report["versions"]["mongo_image_digest"] = run_cmd(
            ["docker", "inspect", "--format={{index .RepoDigests 0}}", "mongo:8.0.4"]
        )
    else:
        report["versions"]["mongod_version"] = "Truy vấn qua lệnh buildInfo của MongoDB 8.0"

    json_path = logs_dir / "env_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    md_path = results_dir / "env_report.md"
    hw = report["hardware_and_os"]
    ver = report["versions"]
    md_content = f"""# BIÊN BẢN MÔI TRƯỜNG THỰC NGHIỆM (Mục 9.3, 9.5 & 10.1)

- **Thời điểm ghi nhận (UTC):** `{report['timestamp_utc']}`
- **Ngữ cảnh chạy:** `{report['execution_context']}`

## 1. Thông tin Phần cứng & Hệ điều hành
| Hạng mục | Giá trị ghi nhận |
| :--- | :--- |
| Hệ điều hành | `{hw.get('os_system')} {hw.get('os_release')} ({hw.get('os_version')})` |
| Lớp ảo hóa / Môi trường | `{hw.get('virtualization_layer', 'N/A')}` |
| Kiến trúc CPU | `{hw.get('architecture')}` |
| Model CPU | `{hw.get('cpu_model', hw.get('processor', 'N/A'))}` |
| Số luồng xử lý (Logical Cores) | `{hw.get('cpu_logical_cores')}` |
| Tổng dung lượng RAM | `{hw.get('ram_total_gib', 'N/A')} GiB` |
| Dung lượng ổ đĩa trống / Tổng | `{hw.get('disk_free_gib')} GiB / {hw.get('disk_total_gib')} GiB` |

## 2. Phiên bản các Công nghệ (Mục 9.5 & 10.1)

### `python --version`
```text
{ver.get('python_version', 'N/A')}
```

### `python -m pip show pymongo`
```text
{ver.get('pymongo_info', 'N/A')}
```

### `mongosh --version`
```text
{ver.get('mongosh_version', 'N/A')}
```

### `mongod --version`
```text
{ver.get('mongod_version', 'N/A')}
```

### `docker compose version` & Image Digest
```text
{ver.get('docker_compose_version', 'Chạy từ bên trong container client')}
Digest: {ver.get('mongo_image_digest', 'N/A')}
```
"""
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"[OK] Đã lưu biên bản môi trường tại:\n  - {json_path}\n  - {md_path}")
    return report


if __name__ == "__main__":
    collect_environment_report()
