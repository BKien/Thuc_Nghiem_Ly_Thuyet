# ==============================================================================
# SCRIPT T? ??NG THI?T L?P M?I TR??NG TH?C NGHI?M CLUSTER TR?N WINDOWS (PowerShell)
# Th?c hi?n ??y ?? Ch??ng 10 (10.1 -> 10.7) & M?c 9.7, 9.8:
#   1. Sinh keyFile x?c th?c n?i b? (config/keyfile/mongo-keyfile)
#   2. Kh?i ch?y Docker Compose (mongo1, mongo2, mongo3, cluster_client)
#   3. Thu th?p bi?n b?n m?i tr??ng (logs/env_report.json & results/env_report.md)
#   4. Kh?i t?o Replica Set rs0 (priority 2/1/1) & t?i kho?n th?c nghi?m
#   5. T?o schema, ch? m?c v? n?p b? d? li?u ch?c n?ng 1.000 h? s? c?ng d?n
#   6. Ki?m tra v? x?c nh?n to?n di?n 8 ?i?u ki?n Cluster ho?t ??ng (M?c 10.7)
# ==============================================================================

$ErrorActionPreference = "Continue"
chcp 65001 > $null
$env:DOCKER_HOST = ""
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

function Check-LastExitCode {
    param([string]$StepName)
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`n[L?I] $StepName th?t b?i (m? l?i: $LASTEXITCODE)!" -ForegroundColor Red
        Write-Host "Vui l?ng ki?m tra log l?i ? tr?n v? kh?c ph?c tr??c khi ch?y ti?p.`n" -ForegroundColor Yellow
        exit $LASTEXITCODE
    }
}

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host " THI?T L?P M?I TR??NG TH?C NGHI?M MONGODB REPLICA SET (rs0)" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

# Ki?m tra tr?ng th?i Docker Desktop
Write-Host "?ang ki?m tra k?t n?i t?i Docker Desktop..." -ForegroundColor Gray
cmd.exe /c "docker info >nul 2>&1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "`n[L?I] Kh?ng th? k?t n?i t?i Docker Engine!" -ForegroundColor Red
    Write-Host "Nguy?n nh?n: Docker Desktop ch?a ???c kh?i ??ng ho?c daemon ch?a s?n s?ng." -ForegroundColor Yellow
    Write-Host "`nH??ng d?n kh?c ph?c:" -ForegroundColor Cyan
    Write-Host "  1. M? ?ng d?ng 'Docker Desktop' t? Start Menu ho?c m?n h?nh Desktop." -ForegroundColor White
    Write-Host "  2. ??i ??n khi bi?u t??ng Docker ? thanh Taskbar ho?c g?c d??i c?a s? Docker Desktop chuy?n sang m?u xanh l? c?y ('Engine running')." -ForegroundColor White
    Write-Host "  3. Ch?y l?i script n?y b?ng l?nh: .\setup.ps1`n" -ForegroundColor White
    exit 1
}
Write-Host "[OK] Docker Engine ?ang ho?t ??ng.`n" -ForegroundColor Green

# B??c 1: T?o t?p .env n?u ch?a c? v? sinh keyFile
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[1/6] ?? t?o t?p .env t? .env.example" -ForegroundColor Green
} else {
    Write-Host "[1/6] ?? t?m th?y t?p c?u h?nh .env" -ForegroundColor Green
}

python scripts/generate_keyfile.py
Check-LastExitCode "B??c 1 (Sinh keyFile)"

# B??c 2: Kh?i ??ng c?m container qua Docker Compose
Write-Host "`n[2/6] ?ang d?ng v? kh?i ??ng c?c container (mongo1, mongo2, mongo3, cluster_client)..." -ForegroundColor Yellow
docker compose up -d --build mongo1 mongo2 mongo3 client
Check-LastExitCode "B??c 2 (Kh?i ??ng c?m container)"

# B??c 3: Kh?i t?o Replica Set rs0 t? container client
Write-Host "`n[3/6] ?ang kh?i t?o Replica Set rs0 v? t?i kho?n th?c nghi?m..." -ForegroundColor Yellow
docker exec cluster_client python scripts/init_cluster.py
Check-LastExitCode "B??c 3 (Kh?i t?o Replica Set rs0)"

# B??c 4: Ghi nh?n bi?n b?n m?i tr??ng
Write-Host "`n[4/6] ?ang thu th?p bi?n b?n m?i tr??ng th?c nghi?m (M?c 9.5 & 10.1)..." -ForegroundColor Yellow
python scripts/collect_env_info.py
Check-LastExitCode "B??c 4 (Thu th?p bi?n b?n m?i tr??ng)"

# B??c 5: Seed b? d? li?u ch?c n?ng 1.000 h? s? c?ng d?n (M?c 9.7 & 9.8)
Write-Host "`n[5/6] ?ang kh?i t?o Schema, Index v? n?p 1.000 h? s? c?ng d?n m?u..." -ForegroundColor Yellow
docker exec cluster_client python scripts/seed_data.py --scale 1000
Check-LastExitCode "B??c 5 (Seed d? li?u 1.000 h? s? c?ng d?n)"

# B??c 6: Ki?m tra v? x?c nh?n 8 ?i?u ki?n Cluster s?n s?ng (M?c 10.7)
Write-Host "`n[6/6] ?ang ki?m tra x?c nh?n tr?ng th?i Cluster (M?c 10.5, 10.6, 10.7)..." -ForegroundColor Yellow
docker exec cluster_client python scripts/verify_cluster.py
Check-LastExitCode "B??c 6 (Ki?m tra x?c nh?n 8 ?i?u ki?n Cluster)"

Write-Host "`n======================================================================" -ForegroundColor Green
Write-Host "[HO?N T?T] M?i tr??ng th?c nghi?m MongoDB Replica Set rs0 ?? s?n s?ng!" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green
Write-Host "B?n c? th? ch?y c?c k?ch b?n Ch??ng 11 b?ng l?nh:" -ForegroundColor Cyan
Write-Host "  docker exec -it cluster_client python scenarios/run_all.py" -ForegroundColor White
Write-Host "Ho?c ch?y t?ng k?ch b?n ??n l?, v? d?:" -ForegroundColor Cyan
Write-Host "  docker exec -it cluster_client python scenarios/kb1_basic_crud.py`n" -ForegroundColor White
