# YouTube Transcript Processor - Python Version

Công cụ lấy transcript từ YouTube, chia đoạn thông minh và tạo quote/caption bằng AI.

## Cài đặt

### 1. Cài dependencies

```bash
pip install mysql-connector-python openai requests youtube-transcript-api
```

### 2. Cấu hình biến môi trường

Tạo file `.env` hoặc export các biến:

```bash
export MYSQL_HOST="localhost"
export MYSQL_PORT="3306"
export MYSQL_USER="root"
export MYSQL_PASSWORD="your_password"
export MYSQL_DATABASE="your_database"

# Tuỳ chọn
export OPENAI_API_KEY="sk-..."  # Fallback nếu Gemini fail
export YOUTUBE_COOKIE_FILE="/path/to/cookies.txt"  # Bypass giới hạn YouTube
```

## Sử dụng

### Chế độ 1: Xử lý video cụ thể theo ID

```bash
python youtube_processor.py SYwXwiuxO-0
```

### Chế độ 2: Xử lý danh sách video từ file

```bash
python youtube_processor.py --file ids.txt
```

File `ids.txt` chứa danh sách video IDs (mỗi dòng 1 ID).

### Chế độ 3: Xử lý tất cả video chưa có quotes

```bash
python youtube_processor.py --all
python youtube_processor.py --all 50  # Giới hạn 50 videos
```

### Chế độ 4: Mặc định (tự động tìm video chưa xử lý)

```bash
python youtube_processor.py
```

## Cải tiến so với phiên bản cũ

### 1. Chia đoạn thông minh hơn
- Không cắt ngang câu đang nói
- Tôn trọng khoảng nghỉ tự nhiên trong speech
- Giữ ngữ cảnh giữa các câu liên quan

### 2. Prompt AI cải tiến
- Cung cấp đầy đủ ngữ cảnh (tiêu đề video, tên sách nếu có)
- Hướng dẫn chi tiết về phong cách viết tự nhiên
- Ví dụ cụ thể cho đầu ra mong muốn
- Yêu cầu rõ ràng về cấu trúc: cảm nhận + trích dẫn

### 3. Validate và đánh giá chất lượng
- Kiểm tra độ dài quote
- Đánh giá score (0-10) dựa trên nhiều yếu tố
- Tự động ẩn quotes kém chất lượng (`is_visible = false`)
- Lọc quotes quá ngắn, quá dài, thiếu ý nghĩa

### 4. Kiến trúc Python thuần
- Không cần Ruby/Rails
- Dễ maintain và mở rộng
- Kết nối MySQL trực tiếp
- Error handling tốt hơn

## Cấu trúc database

Công cụ làm việc với 3 bảng:

- `youtube_videos`: Thông tin video
- `youtube_paragraphs`: Các đoạn văn đã chia
- `youtube_quotes`: Quotes/captions do AI tạo

## Xử lý lỗi

- Tự động retry với fallback (Gemini → OpenAI)
- Log chi tiết từng bước xử lý
- Skip segments quá ngắn
- Đánh dấu quotes kém chất lượng thay vì xóa

## Tuỳ chỉnh

Có thể điều chỉnh trong code:

```python
PARAGRAPH_CONFIG = {
    "max_bytes": 5000,      # Độ dài tối đa 1 paragraph
    "target_bytes": 1800,   # Độ dài mục tiêu
    "min_bytes": 800,       # Độ dài tối thiểu
    "max_segments": 8,      # Số segments tối đa trước khi flush
    "break_gap_seconds": 1.5,  # Khoảng nghỉ để coi là break
}
```

## Lưu ý

- Đảm bảo video có transcript (auto-generated hoặc manual)
- Một số video có thể bị chặn transcript
- Cần kết nối internet để fetch transcript và gọi AI API
