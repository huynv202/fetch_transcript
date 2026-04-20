# YouTube Transcript Processor

Công cụ lấy transcript từ YouTube, chia đoạn có ngữ cảnh và tạo quotes/captions bằng AI (Qwen hoặc OpenAI).

## Cấu trúc project

```
craw_youtube_videos/
├── .env.example          # File mẫu cấu hình môi trường
├── requirements.txt      # Danh sách dependencies
├── youtube_processor.py  # Script chính xử lý toàn bộ
├── ids.txt              # File chứa danh sách video IDs (tuỳ chọn)
└── README.md            # File này
```

## Cài đặt

### 1. Cài đặt dependencies

```bash
cd craw_youtube_videos
pip install -r requirements.txt
```

Hoặc nếu gặp lỗi `externally-managed-environment`:

```bash
# Tạo virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Cấu hình môi trường

Sao chép file mẫu và điền thông tin:

```bash
cp .env.example .env
```

Sửa file `.env` với thông tin của bạn:

```bash
# Cấu hình kết nối MySQL
export MYSQL_HOST="localhost"
export MYSQL_PORT="3306"
export MYSQL_USER="root"
export MYSQL_PASSWORD="your_password_here"
export MYSQL_DATABASE="your_database_name"

# Qwen API Key (DashScope) - Lấy tại https://dashscope.console.aliyun.com/
export DASHSCOPE_API_KEY="sk-your-qwen-key-here"

# (Tuỳ chọn) Cookie YouTube để bypass giới hạn
export YOUTUBE_COOKIE_FILE="/path/to/cookies.txt"

# (Tuỳ chọn) Proxy nếu cần
export HTTP_PROXY="http://proxy.example.com:8080"
export HTTPS_PROXY="https://proxy.example.com:8080"

# (Tuỳ chọn) OpenAI API key làm fallback
export OPENAI_API_KEY="sk-your-openai-key-here"
```

### 3. Load biến môi trường

```bash
source .env
```

Hoặc thêm vào `~/.bashrc` hoặc `~/.zshrc` để load tự động.

## Cách sử dụng

### Xử lý một video

```bash
python youtube_processor.py VIDEO_ID
```

Ví dụ:
```bash
python youtube_processor.py dQw4w9WgXcQ
```

### Xử lý nhiều videos từ file

Tạo file `ids.txt` với mỗi dòng là một video ID:

```
video_id_1
video_id_2
video_id_3
```

Sau đó chạy:
```bash
python youtube_processor.py --file ids.txt
```

### Xử lý tất cả videos chưa có transcript

```bash
python youtube_processor.py --all
```

## Yêu cầu hệ thống

- Python 3.8+
- MySQL database với các bảng:
  - `youtube_videos`: Chứa thông tin videos
  - `youtube_paragraphs`: Chứa transcript đã chia đoạn
  - `youtube_quotes`: Chứa quotes và captions tạo bởi AI

## Cấu trúc database

### youtube_videos
| Field | Type | Description |
|-------|------|-------------|
| id | bigint | Primary key |
| video_id | varchar(255) | YouTube video ID |
| title | varchar(255) | Tiêu đề video |
| description | text | Mô tả video |
| ... | ... | Các fields khác |

### youtube_paragraphs
| Field | Type | Description |
|-------|------|-------------|
| id | bigint | Primary key |
| ordinal_number | int | Số thứ tự đoạn |
| content_raw | text | Nội dung gốc |
| content | text | Nội dung đã xử lý |
| youtube_video_id | bigint | Foreign key |

### youtube_quotes
| Field | Type | Description |
|-------|------|-------------|
| id | bigint | Primary key |
| ordinal_number | int | Số thứ tự quote |
| content | text | JSON chứa quote, caption, score |
| is_visible | tinyint(1) | Có hiển thị không (score >= 6) |
| youtube_video_id | bigint | Foreign key |

## Quy trình xử lý

1. **Kiểm tra video**: Xác minh video tồn tại trong database
2. **Lấy transcript**: Fetch transcript từ YouTube (có fallback qua API)
3. **Chia đoạn**: Chia transcript thành paragraphs có ngữ cảnh
   - Tôn trọng dấu câu (. ! ?)
   - Tôn trọng khoảng nghỉ ≥ 1.5 giây
   - Độ dài phù hợp (800-5000 bytes)
4. **Lưu paragraphs**: Lưu vào database
5. **Tạo quotes & captions**: Gọi AI với đầy đủ ngữ cảnh
   - Quote: Trích xuất câu hay nhất
   - Caption: Viết bài đăng tự nhiên như người đọc sách
   - Score: Đánh giá chất lượng 0-10
6. **Lọc quotes**: Chỉ hiển thị quotes có score >= 6
7. **Lưu results**: Lưu vào database

## API Keys

### Qwen (Khuyến nghị)
- Miễn phí nhiều hơn, hỗ trợ tiếng Việt tốt
- Lấy key tại: https://dashscope.console.aliyun.com/
- Model sử dụng: `qwen-plus`

### OpenAI (Fallback)
- Dự phòng khi Qwen thất bại
- Model sử dụng: `gpt-3.5-turbo`

## Troubleshooting

### Lỗi kết nối MySQL
```bash
# Kiểm tra thông tin kết nối trong .env
# Đảm bảo MySQL đang chạy
sudo systemctl status mysql
```

### Lỗi API key
```bash
# Kiểm tra biến môi trường
echo $DASHSCOPE_API_KEY

# Nếu rỗng, load lại .env
source .env
```

### Lỗi externally-managed-environment
```bash
# Sử dụng virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## License

MIT
