﻿# ==============================================================================
# SCRIPT TỰ ĐỘNG THIẾT LẬP MÔI TRƯỜNG THỰC NGHIỆM CLUSTER TRÊN WINDOWS (PowerShell)
# Thực hiện đầy đủ Chương 10 (10.1 -> 10.7) & Mục 9.7, 9.8:
#   1. Sinh keyFile xác thực nội bộ (config/keyfile/mongo-keyfile)
#   2. Khởi chạy Docker Compose (mongo1, mongo2, mongo3, cluster_client)
#   3. Thu thập biên bản môi trường (logs/env_report.json & results/env_report.md)
#   4. Khởi tạo Replica Set rs0 (priority 2/1/1) & tài khoản thực nghiệm
#   5. Tạo schema, chỉ mục và nạp bộ dữ liệu chức năng 1.000 hồ sơ công dân
#   6. Kiểm tra và xác nhận toàn diện 8 điều kiện Cluster hoạt động (Mục 10.7)
# ==============================================================================

$ErrorActionPreference = "Stop"
$env:DOCKER_HOST = ""
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host " THIẾT LẬP MÔI TRƯỜNG THỰC NGHIỆM MONGODB REPLICA SET (rs0)" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

# Bước 1: Tạo tệp .env nếu chưa có và sinh keyFile
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[1/6] Đã tạo tệp .env từ .env.example" -ForegroundColor Green
} else {
    Write-Host "[1/6] Đã tìm thấy tệp cấu hình .env" -ForegroundColor Green
}

python scripts/generate_keyfile.py

# Bước 2: Khởi động cụm container qua Docker Compose
Write-Host "[2/6] Đang dựng và khởi động các container (mongo1, mongo2, mongo3, cluster_client)..." -ForegroundColor Yellow
docker compose up -d --build mongo1 mongo2 mongo3 client

# Bước 3: Khởi tạo Replica Set rs0 từ container client
Write-Host "[3/6] Đang khởi tạo Replica Set rs0 và tài khoản thực nghiệm..." -ForegroundColor Yellow
docker exec cluster_client python scripts/init_cluster.py

# Bước 4: Ghi nhận biên bản môi trường
Write-Host "[4/6] Đang thu thập biên bản môi trường thực nghiệm (Mục 9.5 & 10.1)..." -ForegroundColor Yellow
python scripts/collect_env_info.py

# Bước 5: Seed bộ dữ liệu chức năng 1.000 hồ sơ công dân (Mục 9.7 & 9.8)
Write-Host "[5/6] Đang khởi tạo Schema, Index và nạp 1.000 hồ sơ công dân mẫu..." -ForegroundColor Yellow
docker exec cluster_client python scripts/seed_data.py --scale 1000

# Bước 6: Kiểm tra và xác nhận 8 điều kiện Cluster sẵn sàng (Mục 10.7)
Write-Host "[6/6] Đang kiểm tra xác nhận trạng thái Cluster (Mục 10.5, 10.6, 10.7)..." -ForegroundColor Yellow
docker exec cluster_client python scripts/verify_cluster.py

Write-Host "`n[HOÀN TẤT] Môi trường thực nghiệm MongoDB Replica Set rs0 đã sẵn sàng!" -ForegroundColor Green
Write-Host "Bạn có thể chạy các kịch bản Chương 11 bằng lệnh:" -ForegroundColor Cyan
Write-Host "  docker exec -it cluster_client python scenarios/run_all.py" -ForegroundColor White
