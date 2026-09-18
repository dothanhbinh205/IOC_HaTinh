# IOC Hà Tĩnh Lakehouse

Pipeline Bronze đọc một hoặc nhiều file JSON từ vùng loading trên MinIO và ghi
vào bảng Apache Iceberg thông qua REST catalog/Apache Polaris.

## Chuẩn bị

1. Sao chép `.env.example` thành `.env` và điền endpoint/credential thực tế.
2. Dùng phiên bản PySpark và các JAR trong `SPARK_PACKAGES` tương thích nhau.
3. Tạo catalog `ICEBERG_WAREHOUSE` trong Polaris và cấp quyền ghi cho principal.

Không commit file `.env` hoặc credential thật vào source control.

## Nạp nhiều JSON theo prefix

Lệnh sau tìm tất cả file `.json`, bao gồm file trong thư mục con. Prefix nên chỉ
chứa data JSON, không đặt manifest JSON chung trong prefix này.

```powershell
.\.venv\Scripts\python.exe -m jobs.bronze `
  --bucket lakehouse `
  --prefix loading/skhcn/ `
  --table ioc.bronze.skhcn_nhiem_vu
```

Job lưu `_source_file` và tự bỏ qua các file đã nạp ở lần chạy trước. Nếu mỗi
file là một JSON array hoặc JSON trình bày trên nhiều dòng, thêm `--multiline`.

## Nạp theo manifest

Đây là cách nên dùng cho lịch chạy production vì một `batch_id` chỉ được nạp
một lần:

```powershell
.\.venv\Scripts\python.exe -m jobs.bronze `
  --bucket lakehouse `
  --manifest-key manifests/skhcn/2026-09-18.json `
  --table ioc.bronze.skhcn_nhiem_vu
```

Manifest chấp nhận cả hai dạng phần tử:

```json
{
  "batch_id": "skhcn-2026-09-18",
  "status": "COMPLETE",
  "files": [
    "loading/skhcn/a.json",
    {"key": "loading/skhcn/subfolder/b.json"}
  ]
}
```

Các file ghi vào cùng một bảng nên thuộc cùng dataset. Job có thể thêm cột mới
vào schema Iceberg, nhưng các cột cùng tên vẫn phải có kiểu dữ liệu tương thích.
