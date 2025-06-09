# MovieLens Data Processing Project

## Cấu trúc thư mục

```
project_root/
├── assignment_tester.py  # File kiểm thử chức năng
├── LoadRatings.py       # File xử lý và tải dữ liệu
├── ml-10m.zip          # Dữ liệu MovieLens 10M dataset
├── ratings.dat         # File dữ liệu ratings đã giải nén
├── README.md           # File hướng dẫn (file này)
└── report.pdf/docx     # Báo cáo kết quả
```

## Yêu cầu hệ thống

- Python 3.x
- MySQL Server
- Thư viện Python cần thiết:
  - mysql-connector-python

## Cách cài đặt

1. Cài đặt các thư viện cần thiết:

```bash
pip install mysql-connector-python
```

2. Giải nén file ml-10m.zip để có file ratings.dat

3. Cấu hình kết nối MySQL trong file LoadRatings.py

## Cách sử dụng

1. Chạy file LoadRatings.py để tải dữ liệu vào MySQL:

```bash
python LoadRatings.py
```

2. Chạy file kiểm thử để kiểm tra kết quả:

```bash
python assignment_tester.py
```
