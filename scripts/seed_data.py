#!/usr/bin/env python3
"""
Script tạo cấu trúc dữ liệu, JSON Schema Validation, bộ chỉ mục (Mục 9.7)
và sinh dữ liệu thử nghiệm tự động với seed cố định (Mục 9.8 & Phụ lục C):
- Database: cluster_lab
- Các collection:
  1. congDan: Hồ sơ công dân hiện tại (_id, maCongDan, hoTen, ngaySinh, gioiTinh,
     trangThai, lienHe, hoGiaDinhId, diaChi, phienBan, updatedAt)
  2. hoGiaDinh: Hộ gia đình (_id, maHo, chuHoId, diaChi, soThanhVien, updatedAt)
  3. cuTru: Lịch sử cư trú (_id, congDanId, diaChi, loaiCuTru, ngayBatDau, ngayKetThuc)
  4. bienDong: Lịch sử biến động (_id, congDanId, loaiBienDong, thoiDiem, giaTriTruoc, giaTriSau)
- Quy mô hỗ trợ:
  + 1.000 công dân (Bộ dữ liệu chức năng - Kịch bản 1 đến 6)
  + 10.000 công dân (Bộ dữ liệu hiệu năng 1 - Kịch bản 7)
  + 100.000 công dân (Bộ dữ liệu hiệu năng 2 - Kịch bản 7)
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, ReadPreference, WriteConcern
from pymongo.errors import CollectionInvalid
from pymongo.read_concern import ReadConcern

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scenarios.common import (
    DB_NAME,
    SEED_VALUE,
    get_direct_client,
    get_rs_client,
    get_standalone_client,
    wait_for_replication_catchup,
)

VUNG_LIST = ["Vung_A", "Vung_B", "Vung_C", "Vung_D"]
QUAN_LIST = ["Quan_1", "Quan_2", "Quan_3", "Quan_4", "Quan_5"]
PHUONG_LIST = ["Phuong_1", "Phuong_2", "Phuong_3", "Phuong_4"]
GIOI_TINH_LIST = ["Nam", "Nu"]


def get_validators() -> dict:
    """Định nghĩa BSON Schema Validation kiểm soát trường bắt buộc và kiểu dữ liệu (Mục 9.7)."""
    dia_chi_schema = {
        "bsonType": "object",
        "required": ["thanhPho", "quan", "phuong", "chiTiet"],
        "properties": {
            "thanhPho": {"bsonType": "string"},
            "quan": {"bsonType": "string"},
            "phuong": {"bsonType": "string"},
            "chiTiet": {"bsonType": "string"},
        },
    }

    return {
        "congDan": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": [
                    "maCongDan",
                    "hoTen",
                    "ngaySinh",
                    "gioiTinh",
                    "trangThai",
                    "diaChi",
                    "phienBan",
                    "updatedAt",
                ],
                "properties": {
                    "maCongDan": {"bsonType": "string"},
                    "hoTen": {"bsonType": "string"},
                    "ngaySinh": {"bsonType": "date"},
                    "gioiTinh": {"bsonType": "string"},
                    "trangThai": {"bsonType": "string"},
                    "lienHe": {"bsonType": "object"},
                    "hoGiaDinhId": {"bsonType": ["objectId", "null"]},
                    "diaChi": dia_chi_schema,
                    "phienBan": {"bsonType": ["int", "long"]},
                    "updatedAt": {"bsonType": "date"},
                },
            }
        },
        "hoGiaDinh": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["maHo", "chuHoId", "diaChi", "updatedAt"],
                "properties": {
                    "maHo": {"bsonType": "string"},
                    "chuHoId": {"bsonType": "objectId"},
                    "diaChi": dia_chi_schema,
                    "soThanhVien": {"bsonType": ["int", "long"]},
                    "updatedAt": {"bsonType": "date"},
                },
            }
        },
        "cuTru": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["congDanId", "diaChi", "loaiCuTru", "ngayBatDau"],
                "properties": {
                    "congDanId": {"bsonType": "objectId"},
                    "diaChi": dia_chi_schema,
                    "loaiCuTru": {"bsonType": "string"},
                    "ngayBatDau": {"bsonType": "date"},
                    "ngayKetThuc": {"bsonType": ["date", "null"]},
                },
            }
        },
        "bienDong": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["congDanId", "loaiBienDong", "thoiDiem"],
                "properties": {
                    "congDanId": {"bsonType": "objectId"},
                    "loaiBienDong": {"bsonType": "string"},
                    "thoiDiem": {"bsonType": "date"},
                    "giaTriTruoc": {"bsonType": ["object", "null"]},
                    "giaTriSau": {"bsonType": ["object", "null"]},
                },
            }
        },
    }


def setup_collections_and_indexes(db, drop_existing: bool = True) -> None:
    validators = get_validators()
    for coll_name, validator in validators.items():
        if drop_existing and coll_name in db.list_collection_names():
            db.drop_collection(coll_name)
        try:
            db.create_collection(
                coll_name,
                validator=validator,
                validationLevel="strict",
                validationAction="error",
            )
        except CollectionInvalid:
            db.command("collMod", coll_name, validator=validator)

    # Bộ chỉ mục nền theo đúng Mục 9.7
    db.congDan.create_index([("maCongDan", ASCENDING)], unique=True, name="idx_unique_maCongDan")
    db.congDan.create_index([("hoGiaDinhId", ASCENDING)], name="idx_hoGiaDinhId")
    db.congDan.create_index(
        [("diaChi.thanhPho", ASCENDING), ("trangThai", ASCENDING)],
        name="idx_thanhPho_trangThai",
    )

    db.hoGiaDinh.create_index([("maHo", ASCENDING)], unique=True, name="idx_unique_maHo")
    db.cuTru.create_index(
        [("congDanId", ASCENDING), ("ngayBatDau", DESCENDING)],
        name="idx_congDanId_ngayBatDau",
    )
    db.bienDong.create_index(
        [("congDanId", ASCENDING), ("thoiDiem", DESCENDING)],
        name="idx_congDanId_thoiDiem",
    )


def seed_dataset(
    scale: int = 1000,
    batch_size: int = 1000,
    target: str = "replica_set",
) -> dict:
    """
    Sinh dữ liệu với seed cố định (Mục 9.8):
    - Số công dân: `scale` (1.000 / 10.000 / 100.000)
    - Số hộ gia đình: `scale // 4` (4 thành viên mỗi hộ)
    - Mỗi công dân có 1 bản ghi cư trú ban đầu và 1 bản ghi biến động đăng ký ban đầu.
    """
    assert scale % 4 == 0, "Quy mô công dân (scale) phải chia hết cho 4 (4 thành viên/hộ)."
    rng = random.Random(SEED_VALUE)
    base_time = datetime(2025, 1, 1, 8, 0, 0, tzinfo=timezone.utc)

    print(f"======================================================================")
    print(f"KHỞI TẠO & SEED DỮ LIỆU THỬ NGHIỆM (Quy mô: {scale:,} công dân | Đích: {target})")
    print(f"======================================================================")

    client_ctx = (
        get_standalone_client()
        if target == "standalone"
        else get_rs_client(use_root=False)
    )

    t_start = time.perf_counter()
    with client_ctx as client:
        wc = (
            WriteConcern(w=1, j=True)
            if target == "standalone"
            else WriteConcern(w="majority", j=True, wtimeout=15000)
        )
        db = client.get_database(
            DB_NAME,
            write_concern=wc,
            read_concern=ReadConcern("majority" if target != "standalone" else "local"),
            read_preference=ReadPreference.PRIMARY,
        )

        print("[1/4] Đang thiết lập Collection, JSON Schema Validation và bộ Index (Mục 9.7)...")
        setup_collections_and_indexes(db, drop_existing=True)

        num_households = scale // 4
        print(f"[2/4] Đang sinh {scale:,} công dân, {num_households:,} hộ gia đình, {scale:,} cư trú...")

        ho_batch, cd_batch, ct_batch, bd_batch = [], [], [], []
        inserted_counts = {"hoGiaDinh": 0, "congDan": 0, "cuTru": 0, "bienDong": 0}

        for h_idx in range(1, num_households + 1):
            ho_id = ObjectId()
            ma_ho = f"HO{h_idx:06d}"
            vung = VUNG_LIST[(h_idx - 1) % len(VUNG_LIST)]
            quan = rng.choice(QUAN_LIST)
            phuong = rng.choice(PHUONG_LIST)
            dia_chi_ho = {
                "thanhPho": vung,
                "quan": quan,
                "phuong": phuong,
                "chiTiet": f"So {h_idx} Khu Dan Cu Mo Phong",
            }

            member_ids = [ObjectId() for _ in range(4)]
            chu_ho_id = member_ids[0]

            ho_doc = {
                "_id": ho_id,
                "maHo": ma_ho,
                "chuHoId": chu_ho_id,
                "diaChi": dia_chi_ho,
                "soThanhVien": 4,
                "updatedAt": base_time,
            }
            ho_batch.append(ho_doc)

            for m_offset, cd_id in enumerate(member_ids):
                cd_num = (h_idx - 1) * 4 + m_offset + 1
                ma_cd = f"TEST{cd_num:06d}"
                # Giữ ví dụ chuẩn ở Mục 9.7 cho TEST000001
                dia_chi_cd = (
                    {
                        "thanhPho": "Vung_A",
                        "quan": "Quan_1",
                        "phuong": "Phuong_1",
                        "chiTiet": "So 1 Khu Dan Cu Mo Phong",
                    }
                    if cd_num == 1
                    else dict(dia_chi_ho)
                )

                birth_days_offset = rng.randint(7000, 25000)
                ngay_sinh = base_time - timedelta(days=birth_days_offset)
                gioi_tinh = GIOI_TINH_LIST[cd_num % 2]

                cd_doc = {
                    "_id": cd_id,
                    "maCongDan": ma_cd,
                    "hoTen": f"Cong dan thu nghiem {cd_num:06d}",
                    "ngaySinh": ngay_sinh,
                    "gioiTinh": gioi_tinh,
                    "trangThai": "dang_quan_ly",
                    "lienHe": {
                        "soDienThoai": f"09{cd_num:08d}"[:10],
                        "email": f"cd{cd_num:06d}@cluster.lab",
                    },
                    "hoGiaDinhId": ho_id,
                    "diaChi": dia_chi_cd,
                    "phienBan": 1,
                    "updatedAt": base_time,
                }
                cd_batch.append(cd_doc)

                ct_doc = {
                    "_id": ObjectId(),
                    "congDanId": cd_id,
                    "diaChi": dia_chi_cd,
                    "loaiCuTru": "thuong_tru",
                    "ngayBatDau": base_time - timedelta(days=365),
                    "ngayKetThuc": None,
                }
                ct_batch.append(ct_doc)

                bd_doc = {
                    "_id": ObjectId(),
                    "congDanId": cd_id,
                    "loaiBienDong": "dang_ky_moi",
                    "thoiDiem": base_time,
                    "giaTriTruoc": None,
                    "giaTriSau": {"trangThai": "dang_quan_ly", "phienBan": 1},
                }
                bd_batch.append(bd_doc)

            if len(cd_batch) >= batch_size:
                db.hoGiaDinh.insert_many(ho_batch, ordered=False)
                db.congDan.insert_many(cd_batch, ordered=False)
                db.cuTru.insert_many(ct_batch, ordered=False)
                db.bienDong.insert_many(bd_batch, ordered=False)
                inserted_counts["hoGiaDinh"] += len(ho_batch)
                inserted_counts["congDan"] += len(cd_batch)
                inserted_counts["cuTru"] += len(ct_batch)
                inserted_counts["bienDong"] += len(bd_batch)
                ho_batch.clear()
                cd_batch.clear()
                ct_batch.clear()
                bd_batch.clear()

        if cd_batch:
            db.hoGiaDinh.insert_many(ho_batch, ordered=False)
            db.congDan.insert_many(cd_batch, ordered=False)
            db.cuTru.insert_many(ct_batch, ordered=False)
            db.bienDong.insert_many(bd_batch, ordered=False)
            inserted_counts["hoGiaDinh"] += len(ho_batch)
            inserted_counts["congDan"] += len(cd_batch)
            inserted_counts["cuTru"] += len(ct_batch)
            inserted_counts["bienDong"] += len(bd_batch)

        elapsed_seed = round(time.perf_counter() - t_start, 3)
        print(f"[3/4] Đã nạp xong dữ liệu qua Primary trong {elapsed_seed}s: {inserted_counts}")

        if target != "standalone":
            print("[4/4] Đang chờ các Secondary bắt kịp hoàn toàn và đối chiếu trên cả 3 node...")
            wait_for_replication_catchup(client, timeout_sec=60)

        # Thu thập thống kê kích thước BSON, dữ liệu và chỉ mục (Mục 9.8)
        coll_stats = {}
        for cname in ["congDan", "hoGiaDinh", "cuTru", "bienDong"]:
            st = db.command("collStats", cname)
            coll_stats[cname] = {
                "count": st.get("count"),
                "avgObjSize_bytes": st.get("avgObjSize"),
                "size_bytes": st.get("size"),
                "storageSize_bytes": st.get("storageSize"),
                "totalIndexSize_bytes": st.get("totalIndexSize"),
                "nindexes": st.get("nindexes"),
            }

        node_verification = {}
        if target != "standalone":
            for idx, node_name in enumerate(["mongo1", "mongo2", "mongo3"]):
                with get_direct_client(idx) as dclient:
                    ndb = dclient.get_database(
                        DB_NAME, read_preference=ReadPreference.SECONDARY_PREFERRED
                    )
                    sample_doc = ndb.congDan.find_one(
                        {"maCongDan": "TEST000001"},
                        {"_id": 0, "maCongDan": 1, "hoTen": 1, "diaChi": 1, "trangThai": 1, "phienBan": 1},
                    )
                    node_verification[node_name] = {
                        "congDan_count": ndb.congDan.count_documents({}),
                        "hoGiaDinh_count": ndb.hoGiaDinh.count_documents({}),
                        "cuTru_count": ndb.cuTru.count_documents({}),
                        "bienDong_count": ndb.bienDong.count_documents({}),
                        "sample_TEST000001": sample_doc,
                    }

    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "scale_congDan": scale,
        "seed": SEED_VALUE,
        "target": target,
        "elapsed_seconds": elapsed_seed,
        "inserted_counts": inserted_counts,
        "collection_stats": coll_stats,
        "node_verification": node_verification,
    }

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    report_path = results_dir / f"seed_report_{target}_{scale}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str, ensure_ascii=False)

    print(f"[OK] Đã lưu báo cáo seed dữ liệu tại: {report_path}")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed dữ liệu thực nghiệm Cluster (Mục 9.7 & 9.8)")
    parser.add_argument(
        "--scale",
        type=int,
        default=1000,
        choices=[1000, 10000, 100000],
        help="Số lượng hồ sơ công dân cần sinh (1000, 10000, hoặc 100000)",
    )
    parser.add_argument(
        "--target",
        type=str,
        default="replica_set",
        choices=["replica_set", "standalone"],
        help="Đích nạp dữ liệu: replica_set (rs0) hoặc standalone (đối chứng P4)",
    )
    args = parser.parse_args()
    seed_dataset(scale=args.scale, target=args.target)
