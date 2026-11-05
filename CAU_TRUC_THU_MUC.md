# GIẢI THÍCH Ý NGHĨA TỪNG THƯ MỤC VÀ TỆP TIN TRONG REPO THỰC NGHIỆM CLUSTER

Tài liệu này giải thích chi tiết cấu trúc thư mục và ý nghĩa của từng tệp tin trong môi trường thực nghiệm **MongoDB Replica Set 3 Node (`rs0`)**, được xây dựng bám sát theo tài liệu **Tìm hiểu và Thử nghiệm Cluster** (Chương 9, Chương 10, Chương 11 và các Phụ lục A–E).

---

## 1. Sơ đồ tổng quan cây thư mục

```text
Thuc_Nghiem_Ly_Thuyet/
├── .env.example                      # Mẫu cấu hình biến môi trường
├── .env                              # Biến môi trường thực tế (cục bộ, không đưa lên Git)
├── .gitignore                        # Quy tắc loại trừ thông tin bí mật, cache, dữ liệu tạm
├── docker-compose.yml                # Định nghĩa hạ tầng container (mongo1, mongo2, mongo3, standalone, client)
├── Dockerfile.client                 # Định nghĩa image cho container client (Python 3.12 + PyMongo 4.x + mongosh)
├── requirements.txt                  # Danh sách thư viện Python và phiên bản phục vụ thực nghiệm
├── setup.ps1                         # Script tự động hóa khởi tạo toàn bộ môi trường trên Windows (PowerShell)
├── setup.sh                          # Script tự động hóa khởi tạo toàn bộ môi trường trên Ubuntu 24.04 / WSL2
├── README.md                         # Hướng dẫn cài đặt, vận hành cụm và chạy các kịch bản thực nghiệm
├── CAU_TRUC_THU_MUC.md               # Tài liệu giải thích chi tiết ý nghĩa từng mục trong thư mục (tệp này)
│
├── config/                           # Thư mục cấu hình tiến trình mongod và bảo mật nội bộ (Phụ lục A)
│   ├── mongod.conf                   # Cấu hình mẫu của mongod (WiredTiger 0.5 GiB, Oplog 512 MiB, Auth, keyFile)
│   ├── docker-entrypoint-keyfile.sh  # Script chuẩn hóa quyền 0400 cho keyFile bên trong container
│   └── keyfile/                      # Thư mục lưu khóa xác thực nội bộ giữa các node
│       └── mongo-keyfile             # Khóa Base64 dùng chung cho mongo1, mongo2, mongo3 (tự động sinh)
│
├── scripts/                          # Thư mục chứa các công cụ triển khai, kiểm tra cụm và nạp dữ liệu (Phụ lục B & C)
│   ├── generate_keyfile.py           # Sinh tệp khóa nội bộ mongo-keyfile chuẩn Base64
│   ├── collect_env_info.py           # Thu thập biên bản môi trường phần cứng & phiên bản phần mềm (Mục 9.5 & 10.1)
│   ├── init_cluster.py               # Khởi tạo Replica Set rs0 (priority 2/1/1) và tạo tài khoản thực nghiệm (Mục 10.3 & 10.4)
│   ├── mongosh_checks.js             # Tập lệnh mongosh kiểm tra trạng thái cụm (rs.status, rs.conf, db.hello, oplog) (Mục 10.5)
│   ├── verify_cluster.py             # Kiểm tra tự động 8 điều kiện xác nhận Cluster sẵn sàng (Mục 10.6 & 10.7)
│   ├── seed_data.py                  # Tạo Schema Validation, Index và sinh dữ liệu 1.000 / 10.000 / 100.000 hồ sơ (Mục 9.7 & 9.8)
│   └── fault_injector.py             # Công cụ giả lập sự cố (stop, kill, start, isolate, reconnect) theo Bảng 11.1
│
├── scenarios/                        # Thư mục chứa 7 kịch bản thử nghiệm thực tế (Chương 11: Mục 11.1 -> 11.7)
│   ├── __init__.py                   # Đánh dấu package Python
│   ├── common.py                     # Thư viện lõi dùng chung (kết nối URI, đồng hồ đơn điệu, phân loại 4 nhóm lỗi, dbHash)
│   ├── kb1_basic_crud.py             # Kịch bản 1: Kiểm tra hoạt động cơ bản của Cluster & CRUD (Mục 11.1 - Bảng 11.3)
│   ├── kb2_replication_sync.py       # Kịch bản 2: Kiểm tra đồng bộ dữ liệu & độ trễ Oplog (Mục 11.2 - Bảng 11.4, 11.5, 11.6)
│   ├── kb3_node_failure.py           # Kịch bản 3: Giả lập một Secondary gặp sự cố & kiểm tra đa số (Mục 11.3 - Bảng 11.7)
│   ├── kb4_failover.py               # Kịch bản 4: Kiểm tra Failover khi Primary dừng đột ngột (Mục 11.4 - Bảng 11.8, 11.9)
│   ├── kb5_recovery.py               # Kịch bản 5: Khôi phục node (5A Catch-up/Takeover, 5B Rollback, 5C Initial Sync) (Mục 11.5 - Bảng 11.10)
│   ├── kb6_consistency.py            # Kịch bản 6: Kiểm tra tính nhất quán dữ liệu & Phiên nhân quả (Mục 11.6 - Bảng 11.11, 11.12)
│   ├── kb7_performance.py            # Kịch bản 7: Kiểm thử hiệu năng P1, P2, P3, P4 (Mục 11.7 - Bảng 11.13 -> 11.16)
│   └── run_all.py                    # Trình điều phối chạy toàn bộ hoặc từng kịch bản từ Kịch bản 1 đến Kịch bản 7
│
├── logs/                             # Thư mục lưu log thô từng thao tác (CSV/JSONL) và lịch sử gây sự cố (Phụ lục D)
└── results/                          # Thư mục xuất bảng kết quả thực nghiệm (Markdown & JSON) phục vụ Chương 11, 12 & Phụ lục E
```

---

## 2. Giải thích chi tiết ý nghĩa từng mục

### 2.1. Nhóm tệp cấu hình hạ tầng ở thư mục gốc (`Root Directory`)

| Tên tệp | Ý nghĩa và vai trò trong thực nghiệm | Liên hệ tài liệu |
| :--- | :--- | :--- |
| [`docker-compose.yml`](file:///D:/Thuc_Nghiem_Ly_Thuyet/docker-compose.yml) | Định nghĩa toàn bộ kiến trúc triển khai trên Docker gồm: 3 container `mongo1`, `mongo2`, `mongo3` thuộc Replica Set `rs0` (mỗi node giới hạn `1 CPU`, `2 GiB RAM`, `WiredTiger cache 0.5 GiB`, `Oplog 512 MiB`, gắn volume riêng tại `/data/db`), 1 container đối chứng `mongo_standalone` (cho cấu hình `P4`), và 1 container `cluster_client` kết nối chung qua mạng nội bộ `cluster_net`. | Mục 9.2, 9.3, 9.4, 9.6, 10.2 & Phụ lục A |
| [`Dockerfile.client`](file:///D:/Thuc_Nghiem_Ly_Thuyet/Dockerfile.client) | Xây dựng môi trường cho container `cluster_client` dựa trên `python:3.12-slim-bookworm`, tích hợp sẵn `mongodb-mongosh`, `docker-cli`, `PyMongo 4.x` và `pandas` để thực thi quản trị cụm, sinh dữ liệu và chạy chương trình tạo tải ngay trong mạng nội bộ Docker. | Mục 9.4, 9.5, 10.2 |
| [`.env.example`](file:///D:/Thuc_Nghiem_Ly_Thuyet/.env.example) & [`.env`](file:///D:/Thuc_Nghiem_Ly_Thuyet/.env) | Quản lý tập trung các tham số môi trường: cố định tag phiên bản nhánh MongoDB 8.0 (`mongo:8.0.4`, không dùng `latest`), tên Replica Set (`rs0`), tên database (`cluster_lab`), thông số bộ nhớ đệm, dung lượng oplog, `SEED_VALUE=42` và thông tin xác thực. | Mục 9.5, 9.6, 10.1 |
| [`.gitignore`](file:///D:/Thuc_Nghiem_Ly_Thuyet/.gitignore) | Đảm bảo nguyên tắc bảo mật tại Mục 9.6 & 10.1: ngăn việc đưa tệp `.env`, khóa nội bộ `mongo-keyfile` và dữ liệu thô vào kho mã nguồn hoặc báo cáo. | Mục 9.6, 10.1 |
| [`requirements.txt`](file:///D:/Thuc_Nghiem_Ly_Thuyet/requirements.txt) | Cố định các thư viện Python phía client: `pymongo` (4.x), `pandas` (tính phân vị p50/p95/p99 và thông lượng), `tabulate` và `python-dotenv`. | Mục 9.4, 9.5, Bảng 11.14 |
| [`setup.ps1`](file:///D:/Thuc_Nghiem_Ly_Thuyet/setup.ps1) & [`setup.sh`](file:///D:/Thuc_Nghiem_Ly_Thuyet/setup.sh) | Kịch bản tự động hóa trọn gói từ bước chuẩn bị keyFile, dựng container, khởi tạo Replica Set `rs0`, thu thập biên bản môi trường, seed 1.000 hồ sơ công dân đến bước xác nhận 8 điều kiện cụm hoạt động thành công. | Chương 10 (10.1 -> 10.7) & Phụ lục B |

---

### 2.2. Thư mục `config/` – Cấu hình MongoDB và Xác thực nội bộ (Phụ lục A)

- [`config/mongod.conf`](file:///D:/Thuc_Nghiem_Ly_Thuyet/config/mongod.conf): Tệp cấu hình tham chiếu chuẩn YAML của tiến trình `mongod`, thể hiện rõ các thiết lập `storage.dbPath: /data/db`, `wiredTiger.engineConfig.cacheSizeGB: 0.5`, `replication.replSetName: rs0`, `replication.oplogSizeMB: 512`, `security.authorization: enabled` và `security.keyFile`.
- [`config/docker-entrypoint-keyfile.sh`](file:///D:/Thuc_Nghiem_Ly_Thuyet/config/docker-entrypoint-keyfile.sh): Script khởi chạy đầu tiên trong mỗi container `mongo1`, `mongo2`, `mongo3`. Có nhiệm vụ sao chép `mongo-keyfile` vào `/etc/mongo-keyfile/mongo-keyfile`, chuẩn hóa ký tự xuống dòng LF, gán quyền `chmod 400` và chủ sở hữu `mongodb:mongodb` để tránh lỗi quyền truy cập tệp khi chạy Docker trên cả Windows/WSL2 lẫn Ubuntu.
- `config/keyfile/`: Thư mục chứa tệp khóa `mongo-keyfile` phục vụ xác thực nội bộ (Internal/Membership Authentication) giữa các thành viên trong Replica Set `rs0`.

---

### 2.3. Thư mục `scripts/` – Công cụ Triển khai, Kiểm tra Cụm & Sinh Dữ liệu (Chương 9, 10 & Phụ lục B, C)

- [`scripts/generate_keyfile.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/generate_keyfile.py): Sinh ngẫu nhiên chuỗi khóa mật mã chuẩn Base64 (756 ký tự) và lưu vào `config/keyfile/mongo-keyfile` để 3 node `mongod` tin cậy lẫn nhau khi trao đổi heartbeat, bầu chọn và sao chép oplog.
- [`scripts/collect_env_info.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/collect_env_info.py): Thực thi các lệnh kiểm tra phiên bản bắt buộc ở Mục 9.5 & 10.1 (`mongod --version`, `mongosh --version`, `docker version`, `docker compose version`, `python --version`, `python -m pip show pymongo`) kèm thông số CPU, RAM, dung lượng ổ đĩa trống và xuất biên bản ra `logs/env_report.json` cùng `results/env_report.md`.
- [`scripts/init_cluster.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/init_cluster.py): Thực hiện khởi tạo Replica Set `rs0` một lần duy nhất (`rs.initiate`) theo đúng cấu hình thành viên tại Mục 5.3, 9.6 & 10.3:
  - `mongo1:27017` (`_id: 0`, `votes: 1`, `priority: 2`)
  - `mongo2:27017` (`_id: 1`, `votes: 1`, `priority: 1`)
  - `mongo3:27017` (`_id: 2`, `votes: 1`, `priority: 1`)
  - `heartbeatIntervalMillis: 2000`, `electionTimeoutMillis: 10000`, `writeConcernMajorityJournalDefault: true`
  - Chờ bầu chọn hoàn tất và khởi tạo tài khoản ứng dụng thực nghiệm (`lab_user`) trên database `cluster_lab`.
- [`scripts/mongosh_checks.js`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/mongosh_checks.js): Tập lệnh chạy bằng công cụ `mongosh` thực thi 5 lệnh kiểm tra quản trị cốt lõi ở Mục 10.5: `rs.status()`, `rs.conf()`, `db.hello()`, `rs.printReplicationInfo()` và `rs.printSecondaryReplicationInfo()`.
- [`scripts/verify_cluster.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/verify_cluster.py): Tự động kiểm chứng toàn bộ **8 điều kiện xác nhận Cluster hoạt động thành công** tại Mục 10.7 (tên `rs0`, 1 Primary + 2 Secondary khỏe mạnh, đúng `votes` & `priority`, khám phá topology từ client, ghi thử nghiệm và đối chiếu trên cả 3 node, volume độc lập, bật xác thực keyFile, ghi nhận cửa sổ oplog) và xuất báo cáo ra `results/cluster_verification.md`.
- [`scripts/seed_data.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/seed_data.py): Hiện thực hóa Mục 9.7 & 9.8 (Phụ lục C):
  - Thiết lập 4 collection nghiệp vụ quản lý nhân khẩu trong database `cluster_lab`: `congDan`, `hoGiaDinh`, `cuTru`, `bienDong` kèm bộ ràng buộc **BSON JSON Schema Validation** và **bộ chỉ mục (Indexes)** chuẩn (`idx_unique_maCongDan`, `idx_unique_maHo`, `idx_hoGiaDinhId`, `idx_thanhPho_trangThai`, `idx_congDanId_ngayBatDau`, `idx_congDanId_thoiDiem`).
  - Sinh dữ liệu tự động với `SEED = 42` cố định ở 3 mức quy mô: **1.000** (bộ chức năng), **10.000** và **100.000** công dân (bộ hiệu năng), tỷ lệ 4 công dân / 1 hộ gia đình, chờ các Secondary bắt kịp hoàn toàn và ghi nhận kích thước BSON trung bình, dung lượng lưu trữ và chỉ mục.
- [`scripts/fault_injector.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scripts/fault_injector.py): Công cụ tạo sự cố theo Bảng 11.1 (`docker stop`, `docker kill`, `docker start`, `docker network disconnect`, `docker network connect`), ghi nhận chính xác mốc thời gian đơn điệu (`perf_counter_ns`) và thời gian thực UTC vào `logs/fault_events.jsonl`.

---

### 2.4. Thư mục `scenarios/` – Bộ 7 Kịch bản Thử nghiệm Cluster (Chương 11)

- [`scenarios/common.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/common.py): Module nền tảng cung cấp:
  - Kết nối Replica Set (`replicaSet=rs0`, `serverSelectionTimeoutMS=30000`, tùy chỉnh `retryWrites`/`retryReads`) và kết nối trực tiếp từng node (`directConnection=true`) để đối chiếu bản sao.
  - Bộ đo thời gian đơn điệu `time.perf_counter_ns()` kết hợp mốc UTC và phân loại kết quả thao tác thành **4 nhóm chuẩn** theo Chương 11: `success` (thành công), `explicit_failure` (thất bại rõ ràng), `write_concern_or_timeout` (lỗi write concern hoặc timeout – chưa chắc chắn), và `no_response` (không phản hồi).
  - Hàm tính toán thống kê độ trễ (`mean`, `p50`, `p95`, `p99`, `max`), kiểm tra cửa sổ oplog, độ trễ sao chép và lấy `dbHash` trên từng node.
- [`scenarios/kb1_basic_crud.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/kb1_basic_crud.py): **Kịch bản 1 (Mục 11.1)** – Kiểm tra topology, khả năng nhận biết Primary của Driver, thực hiện 100 vòng x 4 thao tác CRUD = 400 thao tác (`KB1_000001`..`KB1_000100`) với write concern `"majority"`, đối chiếu trực tiếp dữ liệu trên 3 node và kiểm tra mức xác nhận `w: 3`. Xuất **Bảng 11.3**.
- [`scenarios/kb2_replication_sync.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/kb2_replication_sync.py): **Kịch bản 2 (Mục 11.2)** – Đo độ trễ sao chép từng thay đổi trên 200 mẫu đơn lẻ (ghi `w: 1` trên Primary, đọc chu kỳ 5 ms trên từng Secondary); ghi theo lô 1.000, 10.000 và 50.000 hồ sơ để đo độ trễ cực đại và thời gian hội tụ; đối chiếu số lượng bản ghi, số index và `dbHash` trên cả 3 node. Xuất **Bảng 11.4, 11.5, 11.6**.
- [`scenarios/kb3_node_failure.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/kb3_node_failure.py): **Kịch bản 3 (Mục 11.3)** – Duy trì tải nền đọc/ghi mỗi 50 ms, giả lập sự cố trên một Secondary (`docker kill` và biến thể cô lập mạng `docker network disconnect`), đo thời gian phát hiện lỗi `T1 - T0` qua `rs.status()`, chứng minh cụm vẫn phục vụ đọc/ghi `"majority"` bình thường mà không bầu chọn lại, trong khi thao tác `w: 3` bị timeout sau 5 giây. Xuất **Bảng 11.7**.
- [`scenarios/kb4_failover.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/kb4_failover.py): **Kịch bản 4 (Mục 11.4)** – Chạy tải ghi đánh số liên tục mỗi 20 ms (`KB4_...`), gây sự cố trên Primary ở 3 biến thể (`docker kill` có retry, `docker kill` không retry, `docker stop` có retry), đo thời gian bầu chọn `T2 - T0`, thời gian gián đoạn ghi phía ứng dụng `T3 - T1`, và đối chiếu dữ liệu trên Primary mới để chứng minh 0 thao tác `"majority"` đã xác nhận bị mất. Xuất **Bảng 11.9**.
- [`scenarios/kb5_recovery.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/kb5_recovery.py): **Kịch bản 5 (Mục 11.5)** – Kiểm chứng 3 tình huống khôi phục node:
  - **5A**: Node dừng ngắn hạn bắt kịp qua oplog sau 10.000 thao tác mới và thực hiện `priority takeover` (`R1 - R0`, `R2 - R0`).
  - **5B**: Cô lập Primary cũ và ghi `w: 1`, sau khi tái gia nhập bị `ROLLBACK` (trong khi dữ liệu `"majority"` trên Primary mới được bảo toàn 100%).
  - **5C**: Xóa dữ liệu volume của `mongo3` để kiểm chứng quá trình đồng bộ lại từ đầu (`Initial Sync`: `STARTUP2 -> SECONDARY`). Xuất **Bảng 11.10**.
- [`scenarios/kb6_consistency.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/kb6_consistency.py): **Kịch bản 6 (Mục 11.6)** – Gồm 2 phần:
  - **Phần A**: Đối chiếu toàn diện 3 bản sao sau sự cố (`dbHash`, số bản ghi và từng dòng `(maCongDan, phienBan, updatedAt)`). Xuất **Bảng 11.11**.
  - **Phần B**: Thực hiện 1.000 vòng lặp cập nhật trên Primary rồi đọc ngay trên Secondary để lượng hóa tỷ lệ đọc dữ liệu cũ (`stale reads`) khi dùng `readConcern="local"` không có phiên nhân quả so với khi dùng phiên nhất quán nhân quả (`causalConsistency=True` + `"majority"`), trong cả điều kiện không tải nền và có tải nền. Xuất **Bảng 11.12**.
- [`scenarios/kb7_performance.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/kb7_performance.py): **Kịch bản 7 (Mục 11.7)** – Đo kiểm hiệu năng theo mô hình vòng kín trên 4 cấu hình **P1, P2, P3, P4** (Bảng 11.13), 2 loại tải (chỉ đọc và hỗn hợp 80/20), quy mô 10.000 và 100.000 hồ sơ, ở các mức đồng thời 1, 5, 10 tác vụ (làm nóng 30s, đo 120s), thu thập thông lượng (ops/s), độ trễ p50/p95/p99, tỷ lệ lỗi và % CPU Primary. Xuất **Bảng 11.16**.
- [`scenarios/run_all.py`](file:///D:/Thuc_Nghiem_Ly_Thuyet/scenarios/run_all.py): Điểm vào tổng hợp giúp chạy lần lượt toàn bộ 7 kịch bản hoặc chọn chạy riêng từng kịch bản qua tham số dòng lệnh `--scenario`.

---

### 2.5. Thư mục `logs/` và `results/` – Lưu trữ Nhật ký & Báo cáo Kết quả (Phụ lục D & E)

- `logs/`: Lưu trữ các tệp nhật ký thô phục vụ truy vết và kiểm chứng:
  - `env_report.json`: Dữ liệu JSON chi tiết về cấu hình phần cứng và phiên bản phần mềm.
  - `fault_events.jsonl`: Nhật ký mọi sự kiện gây sự cố (`stop`, `kill`, `start`, `isolate`, `reconnect`) kèm mốc `perf_counter_ns` và UTC.
  - `kb1_crud_operations.csv`, `kb3_*_operations.csv`, `kb4_*_operations.csv`: Nhật ký chi tiết từng yêu cầu của client (mã yêu cầu, thời điểm bắt đầu/kết thúc, độ trễ ms, phân loại trạng thái).
- `results/`: Lưu trữ các bảng kết quả đã tổng hợp dưới định dạng Markdown (`.md`) và JSON (`.json`) khớp trực tiếp với các bảng trống trong Chương 10 và Chương 11 của báo cáo:
  - `env_report.md`: Biên bản môi trường thực nghiệm (Mục 9.3, 9.5, 10.1).
  - `cluster_verification.md`: Biên bản xác nhận 8 điều kiện Cluster hoạt động thành công (Mục 10.7).
  - `seed_report_*.json`: Thống kê số lượng bản ghi, kích thước BSON trung bình, dung lượng dữ liệu và chỉ mục sau khi seed (Mục 9.8).
  - `kb1_results.md` .. `kb7_results.md`: Các bảng số liệu thực tế tương ứng từ **Bảng 11.3** đến **Bảng 11.16**.
