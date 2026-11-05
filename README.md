# MÔI TRƯỜNG THỰC NGHIỆM MONGODB REPLICA SET 3 NODE (`rs0`)

Kho mã nguồn này triển khai đầy đủ môi trường thực nghiệm **Database Cluster (MongoDB Replica Set `rs0` gồm 3 node `mongo1`, `mongo2`, `mongo3`)** cùng bộ công cụ sinh dữ liệu và 7 kịch bản kiểm chứng thực nghiệm theo đúng thiết kế tại Chương 9, Chương 10 và Chương 11 của tài liệu **Tìm hiểu và Thử nghiệm Cluster**.

> Tài liệu giải thích chi tiết ý nghĩa từng thư mục và tệp tin trong dự án được trình bày tại: [**`CAU_TRUC_THU_MUC.md`**](file:///D:/Thuc_Nghiem_Ly_Thuyet/CAU_TRUC_THU_MUC.md).

---

## 1. Kiến trúc môi trường thực nghiệm (Mục 9.2 & 9.6)

| Thành phần | Tên Container / Host | Vai trò & Cấu hình | Giới hạn tài nguyên |
| :--- | :--- | :--- | :--- |
| **Node 1** | `mongo1:27017` | Thành viên `_id: 0`, `votes: 1`, `priority: 2` (Ưu tiên bầu làm Primary), Volume `mongo1_data` | `1 CPU`, `2 GiB RAM`, Cache `0.5 GiB`, Oplog `512 MiB` |
| **Node 2** | `mongo2:27017` | Thành viên `_id: 1`, `votes: 1`, `priority: 1` (Secondary dự phòng), Volume `mongo2_data` | `1 CPU`, `2 GiB RAM`, Cache `0.5 GiB`, Oplog `512 MiB` |
| **Node 3** | `mongo3:27017` | Thành viên `_id: 2`, `votes: 1`, `priority: 1` (Secondary dự phòng), Volume `mongo3_data` | `1 CPU`, `2 GiB RAM`, Cache `0.5 GiB`, Oplog `512 MiB` |
| **Node Đối chứng** | `mongo_standalone:27017` | Instance đơn lẻ đối chứng cho Kịch bản 7 (`P4`), Volume `mongo_standalone_data` | `1 CPU`, `2 GiB RAM`, Cache `0.5 GiB` |
| **Client** | `cluster_client` | Chạy `Python 3.12`, `PyMongo 4.x`, `pandas`, `mongosh` trên cùng mạng nội bộ `cluster_net` | Chia sẻ tài nguyên máy chủ |

---

## 2. Hướng dẫn khởi chạy nhanh (Chương 10)

### Cách 1: Tự động hóa toàn bộ bằng 1 lệnh

- **Trên Windows (PowerShell):**
  ```powershell
  .\setup.ps1
  ```
- **Trên Ubuntu 24.04 LTS / WSL 2:**
  ```bash
  chmod +x setup.sh
  ./setup.sh
  ```

### Cách 2: Chạy từng bước thủ công để quan sát và chụp minh chứng

1. **Sinh `keyFile` xác thực nội bộ (Mục 9.6 & 10.1):**
   ```powershell
   python scripts/generate_keyfile.py
   ```
2. **Khởi động cụm container (Mục 10.2 & 10.4):**
   ```powershell
   docker compose up -d --build mongo1 mongo2 mongo3 client
   ```
3. **Khởi tạo Replica Set `rs0` và tạo tài khoản thực nghiệm (Mục 10.3 & 10.4):**
   ```powershell
   docker exec cluster_client python scripts/init_cluster.py
   ```
4. **Kiểm tra trạng thái bằng `mongosh` (Mục 10.5):**
   ```powershell
   docker exec cluster_client mongosh "mongodb://cluster_admin:ClusterAdminSecret2026!@mongo1:27017,mongo2:27017,mongo3:27017/cluster_lab?authSource=admin&replicaSet=rs0" scripts/mongosh_checks.js
   ```
5. **Tạo Schema, bộ Index và nạp dữ liệu thử nghiệm 1.000 hồ sơ công dân (Mục 9.7 & 9.8):**
   ```powershell
   docker exec cluster_client python scripts/seed_data.py --scale 1000
   ```
6. **Xác nhận 8 điều kiện Cluster hoạt động thành công (Mục 10.7):**
   ```powershell
   docker exec cluster_client python scripts/verify_cluster.py
   ```

---

## 3. Hướng dẫn chạy 7 Kịch bản thử nghiệm (Chương 11)

Tất cả kết quả đo đạc được tự động xuất thành bảng Markdown và JSON trong thư mục `results/` kèm log chi tiết từng thao tác trong `logs/`:

```powershell
# Kịch bản 1: Kiểm tra hoạt động cơ bản và 400 thao tác CRUD (Bảng 11.3)
docker exec cluster_client python scenarios/kb1_basic_crud.py

# Kịch bản 2: Kiểm tra đồng bộ dữ liệu & độ trễ Oplog (Bảng 11.4, 11.5, 11.6)
docker exec cluster_client python scenarios/kb2_replication_sync.py

# Kịch bản 3: Giả lập một Secondary gặp sự cố - kill & cô lập mạng (Bảng 11.7)
docker exec cluster_client python scenarios/kb3_node_failure.py --mode both

# Kịch bản 4: Kiểm tra Failover khi Primary dừng đột ngột (Bảng 11.9)
docker exec cluster_client python scenarios/kb4_failover.py

# Kịch bản 5: Khôi phục node - Oplog catch-up, Priority takeover, Rollback, Initial Sync (Bảng 11.10)
docker exec cluster_client python scenarios/kb5_recovery.py

# Kịch bản 6: Kiểm tra tính nhất quán dữ liệu & Phiên nhất quán nhân quả (Bảng 11.11, 11.12)
docker exec cluster_client python scenarios/kb6_consistency.py

# Kịch bản 7: Kiểm thử hiệu năng P1, P2, P3, P4 (Bảng 11.16)
# (Nếu cần đo cả P4 standalone, bật thêm container standalone: docker compose --profile standalone up -d mongo_standalone)
docker exec cluster_client python scenarios/kb7_performance.py --profiles P1 P2 P3 --concurrencies 1 5 10
```

Hoặc chạy toàn bộ chuỗi kịch bản từ 1 đến 7:
```powershell
docker exec cluster_client python scenarios/run_all.py
```

---

## 4. Giải thích cấu trúc thư mục

Xem chi tiết ý nghĩa từng thư mục và tệp tin tại [**`CAU_TRUC_THU_MUC.md`**](file:///D:/Thuc_Nghiem_Ly_Thuyet/CAU_TRUC_THU_MUC.md):

- **`config/`**: Chứa tệp cấu hình tham chiếu [`mongod.conf`](file:///D:/Thuc_Nghiem_Ly_Thuyet/config/mongod.conf), script entrypoint phân quyền [`docker-entrypoint-keyfile.sh`](file:///D:/Thuc_Nghiem_Ly_Thuyet/config/docker-entrypoint-keyfile.sh) và thư mục `keyfile/` lưu khóa xác thực nội bộ giữa các node.
- **`scripts/`**: Chứa các script thiết lập và quản trị cụm gồm [`generate_keyfile.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/generate_keyfile.py), [`collect_env_info.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/collect_env_info.py), [`init_cluster.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/init_cluster.py), [`mongosh_checks.js`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/mongosh_checks.js), [`verify_cluster.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/verify_cluster.py), [`seed_data.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/seed_data.py) và [`fault_injector.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/fault_injector.py).
- **`scenarios/`**: Chứa thư viện lõi [`common.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/common.py), 7 kịch bản thực nghiệm từ [`kb1_basic_crud.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/kb1_basic_crud.py) đến [`kb7_performance.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/kb7_performance.py) và trình điều phối [`run_all.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/run_all.py).
- **`logs/`**: Lưu nhật ký thao tác chi tiết phía client (`.csv`), sự kiện gây sự cố (`.jsonl`) và biên bản môi trường (`.json`).
- **`results/`**: Lưu báo cáo xác nhận cụm và các bảng kết quả thực nghiệm (`.md`, `.json`) theo đúng cấu trúc bảng của Chương 11.
