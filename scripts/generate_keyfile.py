#!/usr/bin/env python3
"""
Script sinh keyFile chuẩn Base64 phục vụ xác thực nội bộ (Internal Authentication)
giữa các node trong MongoDB Replica Set rs0 (Mục 9.6 & 10.1, 10.2).
Lưu ý: Nội dung keyFile được giữ cục bộ, không đưa vào báo cáo hoặc log.
"""

import base64
import os
import secrets
import stat
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def generate_keyfile(target_path: Path, force: bool = False) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and not force:
        print(f"[INFO] keyFile đã tồn tại tại: {target_path}")
        return target_path

    # MongoDB yêu cầu keyFile từ 6 đến 1024 ký tự thuộc tập Base64
    raw_bytes = secrets.token_bytes(567)  # ~756 ký tự Base64
    key_str = base64.b64encode(raw_bytes).decode("ascii")

    with open(target_path, "w", encoding="ascii", newline="\n") as f:
        f.write(key_str + "\n")

    try:
        os.chmod(target_path, stat.S_IRUSR)
    except Exception:
        pass

    print(f"[OK] Đã tạo keyFile xác thực nội bộ tại: {target_path} (độ dài: {len(key_str)} ký tự)")
    return target_path


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    keyfile_path = project_root / "config" / "keyfile" / "mongo-keyfile"
    generate_keyfile(keyfile_path)
