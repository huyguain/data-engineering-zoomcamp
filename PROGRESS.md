# Tiến độ Data Engineering Zoomcamp

> File cá nhân — không phải tài liệu chính thức của repo. Theo dõi tiến độ học theo `de-learning-roadmap.md`.

## 📅 2026-06-23 (Day 1)

### ✅ Đã hoàn thành

#### Setup GCP

| Item | Giá trị |
|---|---|
| Project ID | `de-zoomcamp-500308` |
| Billing | Firebase Payment account (Direct, no $300 trial — chỉ Always Free tier) |
| Budget | Zero-spend budget, email alert ngay khi vượt $0 |
| APIs enabled | BigQuery, Cloud Storage, IAM Service Account Credentials |
| Service account | `de-zoomcamp-user@de-zoomcamp-500308.iam.gserviceaccount.com` |
| SA roles | BigQuery Admin, Storage Admin |
| JSON key location | `~/.gcp/de-zoomcamp.json` (chmod 600) — **KHÔNG commit, transfer thủ công sang máy khác** |
| gcloud CLI | Đã cài qua snap (`google-cloud-cli --classic`) |

#### Setup local env

`~/.zshrc` đã có:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.gcp/de-zoomcamp.json
```

Verify thành công:
```bash
gcloud config list                # project = de-zoomcamp-500308
gcloud auth list                  # de-zoomcamp-user ACTIVE
bq ls                             # OK
gcloud storage buckets list       # OK
```

#### Đọc tài liệu

- [x] `README.md` (root)
- [x] `de-learning-roadmap.md` — kế hoạch 10 tuần
- [x] `01-docker-terraform/README.md` — overview Module 1
- [x] `01-docker-terraform/docker-sql/01-introduction.md` (skim)

### 🎯 Module 1 — Đang làm

Plan tuần tự `01-docker-terraform/docker-sql/`:

- [ ] `01-introduction.md` — Docker basics, volumes
- [ ] `02-virtual-environment.md` — uv setup
- [ ] `03-dockerizing-pipeline.md` — Viết Dockerfile
- [ ] `04-postgres-docker.md` — Postgres trong Docker
- [ ] `05-data-ingestion.md` — Đọc parquet NYC taxi → Postgres
- [ ] `06-ingestion-script.md` — Notebook → Python script
- [ ] `07-pgadmin.md` — pgAdmin UI
- [ ] `08-dockerizing-ingestion.md` — Đóng gói pipeline
- [ ] `09-docker-compose.md` — Multi-container
- [ ] `10-sql-refresher.md` — SQL practice
- [ ] `11-cleanup.md` — Dọn Docker

Sau đó:
- [ ] Terraform: `terraform/1_terraform_overview.md`
- [ ] Terraform: chạy `terraform/terraform/terraform_basic/` (nhớ edit project ID)
- [ ] Homework: `cohorts/2026/01-docker-terraform/homework.md`

### ⚠️ Lưu ý cá nhân

- **JSON key không có trên repo**. Khi setup máy khác:
  1. Copy file `~/.gcp/de-zoomcamp.json` từ máy cũ qua (USB / encrypted cloud / scp)
  2. `chmod 600 ~/.gcp/de-zoomcamp.json`
  3. Thêm `export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.gcp/de-zoomcamp.json` vào shell rc
  4. `gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS`
  5. `gcloud config set project de-zoomcamp-500308`

- **Khi chạy Terraform**: nhớ `terraform destroy` sau mỗi lab (không có $300 đệm, sai là tốn tiền thật).

- **Region**: dùng `us-central1` / `us-east1` / `us-west1` (Always Free Compute chỉ áp dụng region US).

### 📝 Notes & gotchas

- Firebase Payment billing = pay-as-you-go, không kèm $300 trial. Đã dùng trial trước đó.
- Project đầu tiên `huyguainnt` bị mất quyền access (nhầm Google account?), phải tạo lại `de-zoomcamp-500308`.
- Repo này là clone trực tiếp từ DataTalksClub (upstream). Fork riêng tại `huyguain/data-engineering-zoomcamp` để push tiến độ.
