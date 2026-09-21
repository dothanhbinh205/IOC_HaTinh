# IOC Hà Tĩnh Lakehouse

Pipeline Bronze đọc một hoặc nhiều file JSON từ vùng landing trên MinIO và ghi
vào bảng Apache Iceberg bằng HadoopCatalog. Spark dùng URI
`s3a://lakehouse/iceberg/`, tương ứng với prefix MinIO `s3://lakehouse/iceberg/`.

## Chuẩn bị

1. Sao chép `.env.example` thành `.env` và điền endpoint/credential thực tế.
2. Dùng phiên bản PySpark và các JAR trong `SPARK_PACKAGES` tương thích nhau.
3. Bảo đảm bucket `lakehouse` tồn tại và credential có quyền đọc/ghi/xóa object.

Không commit file `.env` hoặc credential thật vào source control.

## Nạp nhiều JSON theo prefix

Lệnh sau tìm tất cả file `.json`, bao gồm file trong thư mục con. Prefix nên chỉ
chứa data JSON, không đặt manifest JSON chung trong prefix này.

```powershell
.\.venv\Scripts\python.exe -m jobs.bronze `
  --bucket lakehouse `
  --prefix "landing/pg_tancang/incomming_documents/" `
  --table ioc.bronze.incomming_documents
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
    {
      "key": "landing/skhcn/a.json",
      "size_bytes": 12345,
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  ]
}
```

Trước khi khởi tạo Spark, job đối chiếu `size_bytes` bằng metadata của MinIO và
tính lại SHA-256 của toàn bộ object theo dạng streaming. Batch sẽ dừng nếu file
không tồn tại, sai kích thước, sai checksum hoặc thiếu trường kiểm tra toàn vẹn.

Các file ghi vào cùng một bảng nên thuộc cùng dataset. Job có thể thêm cột mới
vào schema Iceberg, nhưng các cột cùng tên vẫn phải có kiểu dữ liệu tương thích.
