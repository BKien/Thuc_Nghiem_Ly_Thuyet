#!/usr/bin/env bash
# ==============================================================================
# SCRIPT TỰ ĐỘNG THIẾT LẬP MÔI TRƯỜNG THỰC NGHIỆM CLUSTER (Ubuntu 24.04 / WSL2)
# Thực hiện đầy đủ Chương 10 (10.1 -> 10.7) & Mục 9.7, 9.8
# ==============================================================================
set -euo pipefail

echo "======================================================================"
echo " THIẾT LẬP MÔI TRƯỜNG THỰC NGHIỆM MONGODB REPLICA SET (rs0)"
echo "======================================================================"

if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "[1/6] Đã tạo tệp .env từ .env.example"
else
    echo "[1/6] Đã tìm thấy tệp cấu hình .env"
fi

python3 scripts/generate_keyfile.py

echo "[2/6] Đang dựng và khởi động các container (mongo1, mongo2, mongo3, cluster_client)..."
docker compose up -d --build mongo1 mongo2 mongo3 client

echo "[3/6] Đang khởi tạo Replica Set rs0 và tài khoản thực nghiệm..."
docker exec cluster_client python scripts/init_cluster.py

echo "[4/6] Đang thu thập biên bản môi trường thực nghiệm (Mục 9.5 & 10.1)..."
python3 scripts/collect_env_info.py

echo "[5/6] Đang khởi tạo Schema, Index và nạp 1.000 hồ sơ công dân mẫu..."
docker exec cluster_client python scripts/seed_data.py --scale 1000

echo "[6/6] Đang kiểm tra xác nhận trạng thái Cluster (Mục 10.5, 10.6, 10.7)..."
docker exec cluster_client python scripts/verify_cluster.py

echo ""
echo "[HOÀN TẤT] Môi trường thực nghiệm MongoDB Replica Set rs0 đã sẵn sàng!"
echo "Chạy các kịch bản Chương 11 bằng lệnh:"
echo "  docker exec -it cluster_client python scenarios/run_all.py"
