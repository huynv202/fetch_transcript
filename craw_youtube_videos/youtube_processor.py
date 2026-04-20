#!/usr/bin/env python3
"""
YouTube Transcript Processor - Phiên bản Python thuần với Qwen AI
Lấy transcript từ YouTube, chia đoạn có ngữ cảnh, tạo quote và caption bằng AI

Cấu trúc project:
craw_youtube_videos/
├── .env.example          # File mẫu cấu hình môi trường
├── requirements.txt      # Danh sách dependencies
├── youtube_processor.py  # Script chính xử lý toàn bộ
├── ids.txt              # File chứa danh sách video IDs (tuỳ chọn)
└── README.md            # Hướng dẫn sử dụng

Cách chạy:
1. Cài đặt dependencies: pip install -r requirements.txt
2. Tạo file .env từ .env.example và điền thông tin
3. Chạy script:
   - python youtube_processor.py [VIDEO_ID]
   - python youtube_processor.py --file ids.txt
   - python youtube_processor.py --all (xử lý tất cả video chưa có transcript)
"""

import os
import sys
import json
import re
import html
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from http.cookiejar import MozillaCookieJar
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path

# Load .env NGAY LAP TUC truoc khi su dung bien moi truong
from dotenv import load_dotenv
load_dotenv()

# Debug: In thong tin de kiem tra
print(f"[DEBUG] MYSQL_HOST: {os.getenv('MYSQL_HOST')}")
print(f"[DEBUG] MYSQL_USER: {os.getenv('MYSQL_USER')}")
print(f"[DEBUG] MYSQL_PASSWORD: {os.getenv('MYSQL_PASSWORD')}")
print(f"[DEBUG] MYSQL_DATABASE: {os.getenv('MYSQL_DATABASE')}")

import mysql.connector
from mysql.connector import Error
import requests

# --- CẤU HÌNH LOGGING ---
import logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Load biến môi trường từ file .env (dùng đường dẫn tuyệt đối)
script_dir = Path(__file__).resolve().parent
env_path = script_dir / '.env'

logger.info(f"🔍 Đang tìm file .env tại: {env_path}")

if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logger.info("✅ Đã tải thành công file .env")
    
    # DEBUG: In ra các biến vừa tải (che mật khẩu)
    db_user = os.getenv('MYSQL_USER')
    db_pass = os.getenv('MYSQL_PASSWORD')
    db_host = os.getenv('MYSQL_HOST')
    db_name = os.getenv('MYSQL_DATABASE')
    
    logger.info(f"📊 DB Host: {db_host}")
    logger.info(f"📊 DB User: {db_user}")
    logger.info(f"📊 DB Pass: {'*' * len(db_pass) if db_pass else 'None'}")
    logger.info(f"📊 DB Name: {db_name}")
    
    if not db_pass:
        logger.error("❌ CẢNH BÁO: MYSQL_PASSWORD không tìm thấy trong .env!")
else:
    logger.error(f"❌ KHÔNG TÌM THẤY file .env tại {env_path}")
    logger.error("Vui lòng tạo file .env từ .env.example")
    sys.exit(1)

# ============================================================================
# CẤU HÌNH
# ============================================================================

SKIP_TEXTS = {"[music]", "[âm nhạc]", "[nhạc]", "[tiếng nhạc]", "♪", "♫", ""}
DEFAULT_LANGS = ["vi", "en"]

# Cấu hình chia đoạn
PARAGRAPH_CONFIG = {
    "max_bytes": 5000,
    "target_bytes": 1800,
    "min_bytes": 800,
    "max_segments": 8,
    "break_gap_seconds": 1.5,
}

# Cấu hình MySQL
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "your_database"),
}

# API Keys
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def log(msg: str, level: str = "INFO"):
    """Ghi log với timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")


def clean_text(text: str) -> str:
    """Làm sạch text: decode HTML entities, chuẩn hóa khoảng trắng"""
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def get_db_connection():
    """Tạo kết nối MySQL"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        if conn.is_connected():
            log("Kết nối MySQL thành công")
            return conn
    except Error as e:
        log(f"Lỗi kết nối MySQL: {e}", "ERROR")
        return None
    return None


# ============================================================================
# YOUTUBE TRANSCRIPT FETCHER
# ============================================================================


class YouTubeTranscriptFetcher:
    """Class để lấy transcript từ YouTube"""

    def __init__(self, cookie_file: Optional[str] = None):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
        )

        if cookie_file and os.path.exists(cookie_file):
            cookie_jar = MozillaCookieJar()
            cookie_jar.load(cookie_file, ignore_discard=True, ignore_expires=True)
            self.session.cookies = cookie_jar
            log(f"Đã load cookies từ {cookie_file}")

    def fetch_transcript(self, video_id: str, langs: List[str] = None) -> Optional[List[Dict]]:
        """
        Lấy transcript từ YouTube
        
        Args:
            video_id: ID của video YouTube
            langs: Danh sách ngôn ngữ ưu tiên
            
        Returns:
            List of dicts với keys: text, start, duration
            hoặc None nếu không tìm thấy
        """
        if langs is None:
            langs = DEFAULT_LANGS

        log(f"Đang lấy transcript cho video {video_id}, languages: {langs}")

        try:
            # Bước 1: Lấy watch page để tìm transcript URL
            watch_url = f"https://www.youtube.com/watch?v={video_id}"
            response = self.session.get(watch_url, timeout=10)
            response.raise_for_status()

            # Tìm transcript URL trong page
            transcript_url = self._extract_transcript_url(response.text)
            if not transcript_url:
                log(f"Không tìm thấy transcript URL cho video {video_id}", "WARNING")
                return None

            # Bước 2: Lấy transcript từ URL
            transcript_response = self.session.get(transcript_url, timeout=10)
            transcript_response.raise_for_status()

            # Parse XML transcript
            segments = self._parse_transcript_xml(transcript_response.text, langs)
            
            if segments:
                log(f"Đã lấy được {len(segments)} segments cho video {video_id}")
                return segments
            else:
                log(f"Không có segment nào phù hợp ngôn ngữ cho video {video_id}", "WARNING")
                return None

        except requests.RequestException as e:
            log(f"Lỗi HTTP khi lấy transcript: {e}", "ERROR")
            return None
        except Exception as e:
            log(f"Lỗi không xác định khi lấy transcript: {e}", "ERROR")
            return None

    def _extract_transcript_url(self, html_content: str) -> Optional[str]:
        """Trích xuất transcript URL từ HTML watch page"""
        # Tìm transcript baseUrl trong captions array
        patterns = [
            r'"baseUrl":"([^"]+)"',
            r'"transcriptUrl":"([^"]+)"',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, html_content)
            for match in matches:
                url = match.replace("\\u0026", "&")
                if "transcript" in url or "timedtext" in url:
                    return url

        # Thử tìm trong JSON data
        try:
            json_start = html_content.find('"captions":')
            if json_start != -1:
                json_end = html_content.find("};", json_start) + 1
                if json_end > json_start:
                    json_str = html_content[json_start:json_end]
                    data = json.loads(json_str)
                    if "captions" in data:
                        renderer = data["captions"].get("playerCaptionsTracklistRenderer", {})
                        tracks = renderer.get("captionTracks", [])
                        for track in tracks:
                            if "baseUrl" in track:
                                return track["baseUrl"]
        except (json.JSONDecodeError, KeyError):
            pass

        return None

    def _parse_transcript_xml(self, xml_content: str, langs: List[str]) -> List[Dict]:
        """Parse XML transcript và lọc theo ngôn ngữ"""
        try:
            root = ET.fromstring(xml_content)
            segments = []

            # Kiểm tra xem có phải là list ngôn ngữ không
            if root.tag == "transcript_list":
                # Tìm track phù hợp với ngôn ngữ ưu tiên
                for lang in langs:
                    track = root.find(f'.//track[@lang_code="{lang}"]')
                    if track is not None and "url" in track.attrib:
                        # Load transcript từ URL của track đó
                        url = track.attrib["url"]
                        response = self.session.get(url, timeout=10)
                        response.raise_for_status()
                        return self._parse_transcript_xml(response.text, [])

            elif root.tag == "transcript":
                for child in root:
                    if child.tag == "text":
                        text = child.text or ""
                        if text.strip() and text.strip().lower() not in SKIP_TEXTS:
                            start = float(child.get("start", 0))
                            duration = float(child.get("dur", 0))
                            segments.append({
                                "text": clean_text(text),
                                "start": start,
                                "duration": duration,
                            })

            return segments

        except ET.ParseError as e:
            log(f"Lỗi parse XML transcript: {e}", "ERROR")
            return []

    def fetch_from_api(self, video_id: str, langs: List[str] = None) -> Optional[List[Dict]]:
        """Fallback: Sử dụng youtube-transcript-api nếu có"""
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
            
            if langs is None:
                langs = DEFAULT_LANGS

            log(f"Đang thử youtube-transcript-api cho video {video_id}")
            
            # Thử lấy transcript với language preferences
            for lang in langs:
                try:
                    transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=[lang])
                    if transcript:
                        segments = [
                            {
                                "text": clean_text(seg["text"]),
                                "start": seg["start"],
                                "duration": seg["duration"],
                            }
                            for seg in transcript
                            if seg["text"].strip().lower() not in SKIP_TEXTS
                        ]
                        if segments:
                            log(f"Đã lấy được {len(segments)} segments qua API")
                            return segments
                except Exception:
                    continue

            # Thử lấy transcript mặc định
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            segments = [
                {
                    "text": clean_text(seg["text"]),
                    "start": seg["start"],
                    "duration": seg["duration"],
                }
                for seg in transcript
                if seg["text"].strip().lower() not in SKIP_TEXTS
            ]
            
            if segments:
                log(f"Đã lấy được {len(segments)} segments qua API (default)")
                return segments

            return None

        except ImportError:
            log("youtube-transcript-api không được cài đặt", "WARNING")
            return None
        except Exception as e:
            log(f"Lỗi khi dùng youtube-transcript-api: {e}", "ERROR")
            return None


# ============================================================================
# PARAGRAPH SPLITTER
# ============================================================================


class ParagraphSplitter:
    """Chia transcript thành các đoạn có ý nghĩa với ngữ cảnh"""

    def __init__(self, config: Dict = None):
        self.config = config or PARAGRAPH_CONFIG

    def split(self, segments: List[Dict]) -> List[Dict]:
        """
        Chia segments thành paragraphs có ngữ cảnh
        
        Logic:
        1. Gom các segments liên tiếp thành paragraph
        2. Không cắt ngang câu (tôn trọng dấu câu: . ! ?)
        3. Tôn trọng khoảng nghỉ >= break_gap_seconds
        4. Đảm bảo mỗi paragraph có độ dài phù hợp (min_bytes - max_bytes)
        
        Returns:
            List of dicts với keys: content_raw, ordinal_number
        """
        if not segments:
            return []

        paragraphs = []
        current_paragraph = []
        current_length = 0

        for i, segment in enumerate(segments):
            text = segment["text"]
            duration = segment["duration"]
            start = segment["start"]
            
            # Tính độ dài byte (UTF-8)
            text_bytes = len(text.encode("utf-8"))
            
            # Kiểm tra xem có nên bắt đầu paragraph mới không
            should_break = False
            
            # Nếu paragraph hiện tại đã vượt max_bytes
            if current_length + text_bytes > self.config["max_bytes"]:
                should_break = True
            
            # Nếu có khoảng nghỉ lớn giữa các segments
            if i > 0 and current_paragraph:
                prev_segment = segments[i - 1]
                gap = start - (prev_segment["start"] + prev_segment["duration"])
                if gap >= self.config["break_gap_seconds"]:
                    should_break = True
            
            # Nếu đã đến target_bytes và gặp dấu câu kết thúc câu
            if current_length >= self.config["target_bytes"]:
                if text.endswith((".", "!", "?", ".\"", "!\"", "?\"")):
                    should_break = True
            
            # Break nếu cần
            if should_break and current_paragraph:
                # Chỉ break nếu đang ở cuối câu hoặc gần cuối câu
                if self._is_good_break_point(current_paragraph):
                    paragraphs.append(self._build_paragraph(paragraphs, current_paragraph))
                    current_paragraph = []
                    current_length = 0
            
            # Thêm segment vào paragraph hiện tại
            current_paragraph.append(segment)
            current_length += text_bytes

        # Thêm paragraph cuối cùng
        if current_paragraph:
            paragraphs.append(self._build_paragraph(paragraphs, current_paragraph))

        log(f"Đã chia thành {len(paragraphs)} paragraphs")
        return paragraphs

    def _is_good_break_point(self, segments: List[Dict]) -> bool:
        """Kiểm tra xem có phải điểm break tốt không (cuối câu)"""
        if not segments:
            return True
        
        last_text = segments[-1]["text"]
        return last_text.rstrip().endswith((".", "!", "?", ".\"", "!\"", "?\"", "..."))

    def _build_paragraph(self, existing_paragraphs: List[Dict], segments: List[Dict]) -> Dict:
        """Xây dựng paragraph từ segments"""
        ordinal = len(existing_paragraphs) + 1
        
        # Nối các segments lại
        texts = [seg["text"] for seg in segments]
        content_raw = " ".join(texts)
        
        # Chuẩn hóa khoảng trắng và dấu câu
        content_raw = re.sub(r"\s+", " ", content_raw).strip()
        content_raw = re.sub(r"\s+([.,!?;:])", r"\1", content_raw)
        
        return {
            "ordinal_number": ordinal,
            "content_raw": content_raw,
        }


# ============================================================================
# AI PROCESSOR (Qwen + Fallback OpenAI)
# ============================================================================


class AIProcessor:
    """Xử lý AI để tạo quotes và captions với Qwen làm primary"""

    def __init__(self):
        self.qwen_key = DASHSCOPE_API_KEY
        self.openai_key = OPENAI_API_KEY
        
        if not self.qwen_key and not self.openai_key:
            log("Cảnh báo: Không có API key nào (Qwen hoặc OpenAI)", "WARNING")

    def create_quote_and_caption(self, paragraph: str, context_before: str = "", 
                                  context_after: str = "", video_title: str = "",
                                  video_description: str = "") -> Optional[Dict]:
        """
        Tạo quote và caption từ paragraph với ngữ cảnh
        
        Args:
            paragraph: Đoạn văn cần trích xuất quote
            context_before: Ngữ cảnh trước (1-2 paragraphs)
            context_after: Ngữ cảnh sau (1 paragraph)
            video_title: Tiêu đề video
            video_description: Mô tả video
            
        Returns:
            Dict với keys: quote, caption, score (0-10)
            hoặc None nếu thất bại
        """
        prompt = self._build_prompt(paragraph, context_before, context_after, 
                                    video_title, video_description)
        
        # Thử Qwen trước
        result = self._call_qwen(prompt)
        if result:
            return result
        
        # Fallback sang OpenAI
        if self.openai_key:
            log("Qwen thất bại, đang thử OpenAI...", "WARNING")
            return self._call_openai(prompt)
        
        log("Không có AI available, trả về default", "WARNING")
        return self._create_default(paragraph)

    def _build_prompt(self, paragraph: str, context_before: str, context_after: str,
                      video_title: str, video_description: str) -> str:
        """Xây dựng prompt chi tiết cho AI"""
        
        prompt = f"""Bạn là một chuyên gia về sách và content creator, nhiệm vụ của bạn là trích xuất những câu nói hay (quotes) từ đoạn văn dưới đây và viết một bài đăng chia sẻ cảm xúc tự nhiên như một người đọc sách.

## THÔNG TIN VIDEO
**Tiêu đề:** {video_title if video_title else "Không có"}
**Mô tả:** {video_description[:500] if video_description else "Không có"}

## NGỮ CẢNH
**Đoạn trước:** {context_before[:500] if context_before else "Không có"}

**Đoạn hiện tại:** {paragraph}

**Đoạn sau:** {context_after[:500] if context_after else "Không có"}

## YÊU CẦU

### 1. TRÍCH XUẤT QUOTE
Chọn MỘT câu hoặc đoạn ngắn (tối đa 3 dòng) hay nhất, sâu sắc nhất từ đoạn hiện tại. Quote phải:
- Có ý nghĩa độc lập, dễ hiểu
- Truyền cảm hứng hoặc gợi suy ngẫm
- Đúng nguyên văn từ transcript

### 2. VIẾT CAPTION
Viết một bài đăng Facebook/Instagram (200-400 từ) chia sẻ về quote này với giọng văn:
- **Tự nhiên, chân thật** như một người đọc sách thực sự
- **Có cảm xúc**: Thể hiện sự đồng cảm, suy ngẫm cá nhân
- **Có ngữ cảnh**: Giải thích ngắn gọn bối cảnh của quote trong câu chuyện
- **Thu hút**: Mở đầu ấn tượng, kết thúc gợi mở
- **Không sáo rỗng**: Tránh những câu chung chung như "rất hay", "ý nghĩa"

### 3. ĐÁNH GIÁ CHẤT LƯỢNG (Score 0-10)
Chấm điểm dựa trên:
- Quote có hay và ý nghĩa không?
- Caption có tự nhiên, chân thật không?
- Có đủ ngữ cảnh để hiểu không?

## ĐỊNH DẠNG OUTPUT (JSON)
{{
    "quote": "trích dẫn nguyên văn quote",
    "caption": "nội dung caption đầy đủ",
    "score": 8.5,
    "reasoning": "lý do chấm điểm này"
}}

Hãy phân tích kỹ và đưa ra kết quả chất lượng cao nhất."""

        return prompt

    def _call_qwen(self, prompt: str) -> Optional[Dict]:
        """Gọi Qwen API qua DashScope"""
        if not self.qwen_key:
            return None
        
        try:
            log("Đang gọi Qwen API...")
            
            headers = {
                "Authorization": f"Bearer {self.qwen_key}",
                "Content-Type": "application/json",
            }
            
            payload = {
                "model": "qwen-plus",
                "input": {
                    "messages": [
                        {
                            "role": "system",
                            "content": "Bạn là một chuyên gia về sách và content creator. Hãy trả lời bằng JSON."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                },
                "parameters": {
                    "temperature": 0.7,
                    "max_tokens": 1000,
                    "result_format": "message"
                }
            }
            
            response = requests.post(
                "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            
            if result.get("status_code") == 200:
                content = result["output"]["choices"][0]["message"]["content"]
                return self._parse_ai_response(content)
            else:
                log(f"Qwen API error: {result}", "ERROR")
                return None
                
        except requests.RequestException as e:
            log(f"Lỗi gọi Qwen API: {e}", "ERROR")
            return None
        except Exception as e:
            log(f"Lỗi xử lý Qwen response: {e}", "ERROR")
            return None

    def _call_openai(self, prompt: str) -> Optional[Dict]:
        """Gọi OpenAI API (fallback)"""
        if not self.openai_key:
            return None
        
        try:
            log("Đang gọi OpenAI API...")
            
            client = OpenAI(api_key=self.openai_key)
            
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system",
                        "content": "Bạn là một chuyên gia về sách và content creator. Hãy trả lời bằng JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.7,
                max_tokens=1000,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            return self._parse_ai_response(content)
            
        except Exception as e:
            log(f"Lỗi gọi OpenAI API: {e}", "ERROR")
            return None

    def _parse_ai_response(self, content: str) -> Optional[Dict]:
        """Parse JSON response từ AI"""
        try:
            # Tìm JSON trong response
            json_match = re.search(r"\{[^{}]*\}", content, re.DOTALL)
            if json_match:
                content = json_match.group()
            
            data = json.loads(content)
            
            # Validate required fields
            if "quote" not in data or "caption" not in data:
                log("AI response thiếu quote hoặc caption", "WARNING")
                return None
            
            # Default score nếu không có
            if "score" not in data:
                data["score"] = 7.0
            if "reasoning" not in data:
                data["reasoning"] = ""
            
            # Làm sạch
            data["quote"] = clean_text(str(data["quote"]))
            data["caption"] = clean_text(str(data["caption"]))
            
            log(f"AI tạo quote thành công, score: {data['score']}")
            return data
            
        except json.JSONDecodeError as e:
            log(f"Lỗi parse JSON từ AI: {e}", "ERROR")
            return None

    def _create_default(self, paragraph: str) -> Dict:
        """Tạo quote/caption default khi không có AI"""
        # Lấy câu đầu tiên làm quote
        sentences = re.split(r"(?<=[.!?])\s+", paragraph)
        quote = sentences[0] if sentences else paragraph[:200]
        
        caption = f"Một đoạn trích hay:\n\n\"{quote}\"\n\nTừ nội dung trên, chúng ta có thể suy ngẫm về nhiều điều thú vị."
        
        return {
            "quote": quote,
            "caption": caption,
            "score": 5.0,
            "reasoning": "Default fallback"
        }


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================


class DatabaseManager:
    """Quản lý thao tác database"""

    def __init__(self, connection):
        self.conn = connection

    def video_exists(self, video_id: str) -> Optional[int]:
        """Kiểm tra video đã tồn tại trong DB chưa, trả về id"""
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT id FROM youtube_videos WHERE video_id = %s",
                (video_id,)
            )
            result = cursor.fetchone()
            cursor.close()
            return result["id"] if result else None
        except Error as e:
            log(f"Lỗi kiểm tra video: {e}", "ERROR")
            return None

    def has_transcript(self, video_db_id: int) -> bool:
        """Kiểm tra video đã có transcript chưa"""
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT COUNT(*) as count FROM youtube_paragraphs WHERE youtube_video_id = %s",
                (video_db_id,)
            )
            result = cursor.fetchone()
            cursor.close()
            return result["count"] > 0
        except Error as e:
            log(f"Lỗi kiểm tra transcript: {e}", "ERROR")
            return False

    def save_paragraphs(self, video_db_id: int, paragraphs: List[Dict]):
        """Lưu paragraphs vào database"""
        try:
            cursor = self.conn.cursor()
            
            for para in paragraphs:
                cursor.execute(
                    """INSERT INTO youtube_paragraphs 
                       (ordinal_number, content_raw, content, youtube_video_id, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, NOW(6), NOW(6))""",
                    (
                        para["ordinal_number"],
                        para["content_raw"],
                        para.get("content", para["content_raw"]),
                        video_db_id,
                    )
                )
            
            self.conn.commit()
            cursor.close()
            log(f"Đã lưu {len(paragraphs)} paragraphs")
            
        except Error as e:
            log(f"Lỗi lưu paragraphs: {e}", "ERROR")
            self.conn.rollback()

    def save_quotes(self, video_db_id: int, quotes: List[Dict]):
        """Lưu quotes vào database"""
        try:
            cursor = self.conn.cursor()
            
            for quote_data in quotes:
                cursor.execute(
                    """INSERT INTO youtube_quotes 
                       (ordinal_number, content, is_visible, youtube_video_id, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, NOW(6), NOW(6))""",
                    (
                        quote_data["ordinal_number"],
                        quote_data["content"],
                        quote_data.get("is_visible", 1),
                        video_db_id,
                    )
                )
            
            self.conn.commit()
            cursor.close()
            log(f"Đã lưu {len(quotes)} quotes")
            
        except Error as e:
            log(f"Lỗi lưu quotes: {e}", "ERROR")
            self.conn.rollback()

    def get_all_video_ids(self) -> List[Tuple[int, str]]:
        """Lấy tất cả video IDs từ database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, video_id FROM youtube_videos ORDER BY created_at DESC")
            results = cursor.fetchall()
            cursor.close()
            return [(row[0], row[1]) for row in results]
        except Error as e:
            log(f"Lỗi lấy video IDs: {e}", "ERROR")
            return []

    def get_video_info(self, video_db_id: int) -> Dict:
        """Lấy thông tin video"""
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT * FROM youtube_videos WHERE id = %s",
                (video_db_id,)
            )
            result = cursor.fetchone()
            cursor.close()
            return result or {}
        except Error as e:
            log(f"Lỗi lấy thông tin video: {e}", "ERROR")
            return {}

    def get_paragraphs_for_video(self, video_db_id: int) -> List[Dict]:
        """Lấy tất cả paragraphs của video"""
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT * FROM youtube_paragraphs WHERE youtube_video_id = %s ORDER BY ordinal_number",
                (video_db_id,)
            )
            results = cursor.fetchall()
            cursor.close()
            return results
        except Error as e:
            log(f"Lỗi lấy paragraphs: {e}", "ERROR")
            return []


# ============================================================================
# MAIN PROCESSOR
# ============================================================================


class YouTubeProcessor:
    """Class chính để xử lý toàn bộ quy trình"""

    def __init__(self):
        self.conn = get_db_connection()
        if not self.conn:
            raise Exception("Không thể kết nối database")
        
        self.db = DatabaseManager(self.conn)
        self.transcript_fetcher = YouTubeTranscriptFetcher(
            cookie_file=os.getenv("YOUTUBE_COOKIE_FILE")
        )
        self.paragraph_splitter = ParagraphSplitter()
        self.ai_processor = AIProcessor()

    def process_video(self, video_id: str) -> bool:
        """
        Xử lý một video hoàn chỉnh
        
        Returns:
            True nếu thành công, False nếu thất bại
        """
        log(f"{'='*60}")
        log(f"Bắt đầu xử lý video: {video_id}")
        log(f"{'='*60}")

        try:
            # Bước 1: Kiểm tra video tồn tại trong DB
            video_db_id = self.db.video_exists(video_id)
            if not video_db_id:
                log(f"Video {video_id} không tồn tại trong database", "ERROR")
                return False

            # Bước 2: Kiểm tra đã có transcript chưa
            if self.db.has_transcript(video_db_id):
                log(f"Video {video_id} đã có transcript, bỏ qua", "WARNING")
                return True

            # Bước 3: Lấy thông tin video
            video_info = self.db.get_video_info(video_db_id)
            video_title = video_info.get("title", "")
            video_description = video_info.get("description", "")

            # Bước 4: Lấy transcript
            segments = self.transcript_fetcher.fetch_transcript(video_id)
            
            if not segments:
                # Thử fallback qua API
                log("Thử fallback qua youtube-transcript-api...")
                segments = self.transcript_fetcher.fetch_from_api(video_id)
            
            if not segments:
                log(f"Không lấy được transcript cho video {video_id}", "ERROR")
                return False

            # Bước 5: Chia paragraphs
            paragraphs = self.paragraph_splitter.split(segments)
            
            if not paragraphs:
                log("Không chia được paragraphs", "ERROR")
                return False

            # Bước 6: Lưu paragraphs
            self.db.save_paragraphs(video_db_id, paragraphs)

            # Bước 7: Tạo quotes và captions với AI
            all_quotes = []
            all_paragraphs_data = self.db.get_paragraphs_for_video(video_db_id)
            
            for i, para_record in enumerate(all_paragraphs_data):
                para_text = para_record["content_raw"]
                
                # Lấy ngữ cảnh
                context_before = ""
                context_after = ""
                
                if i > 0:
                    context_before = all_paragraphs_data[i-1]["content_raw"]
                if i < len(all_paragraphs_data) - 1:
                    context_after = all_paragraphs_data[i+1]["content_raw"]
                
                # Gọi AI
                log(f"Đang xử lý paragraph {i+1}/{len(all_paragraphs_data)}...")
                ai_result = self.ai_processor.create_quote_and_caption(
                    paragraph=para_text,
                    context_before=context_before,
                    context_after=context_after,
                    video_title=video_title,
                    video_description=video_description
                )
                
                if ai_result:
                    # Chỉ lưu quotes có score >= 6
                    is_visible = 1 if ai_result["score"] >= 6 else 0
                    
                    quote_record = {
                        "ordinal_number": i + 1,
                        "content": json.dumps({
                            "quote": ai_result["quote"],
                            "caption": ai_result["caption"],
                            "score": ai_result["score"],
                            "reasoning": ai_result["reasoning"]
                        }, ensure_ascii=False),
                        "is_visible": is_visible,
                    }
                    all_quotes.append(quote_record)
                    
                    log(f"Quote {i+1}: score={ai_result['score']}, visible={is_visible}")

            # Bước 8: Lưu quotes
            if all_quotes:
                self.db.save_quotes(video_db_id, all_quotes)
                visible_count = sum(1 for q in all_quotes if q["is_visible"])
                log(f"Tổng cộng: {len(all_quotes)} quotes, {visible_count} quotes hiển thị")

            log(f"Xử lý thành công video {video_id}!")
            return True

        except Exception as e:
            log(f"Lỗi không xác định khi xử lý video {video_id}: {e}", "ERROR")
            import traceback
            traceback.print_exc()
            return False

    def process_batch(self, video_ids: List[str]) -> Dict[str, bool]:
        """Xử lý hàng loạt videos"""
        results = {}
        
        for i, video_id in enumerate(video_ids, 1):
            log(f"\n{'='*60}")
            log(f"Processing {i}/{len(video_ids)}: {video_id}")
            log(f"{'='*60}")
            
            success = self.process_video(video_id)
            results[video_id] = success
            
            # Pause nhẹ giữa các videos
            if i < len(video_ids):
                import time
                time.sleep(2)
        
        return results

    def close(self):
        """Đóng kết nối database"""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            log("Đã đóng kết nối database")


# ============================================================================
# CLI ENTRY POINT
# ============================================================================


def main():
    """Hàm main cho CLI"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="YouTube Transcript Processor - Lấy transcript, chia đoạn, tạo quotes bằng AI"
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("video_id", nargs="?", help="YouTube Video ID để xử lý")
    group.add_argument("--file", "-f", help="File chứa danh sách video IDs (mỗi ID một dòng)")
    group.add_argument("--all", "-a", action="store_true", help="Xử lý tất cả videos chưa có transcript")
    
    args = parser.parse_args()
    
    # Xác định danh sách videos cần xử lý
    video_ids = []
    
    if args.video_id:
        video_ids = [args.video_id]
    elif args.file:
        if not os.path.exists(args.file):
            log(f"File {args.file} không tồn tại", "ERROR")
            sys.exit(1)
        
        with open(args.file, "r") as f:
            video_ids = [line.strip() for line in f if line.strip()]
        
        log(f"Đã đọc {len(video_ids)} video IDs từ {args.file}")
    elif args.all:
        processor = YouTubeProcessor()
        all_videos = processor.db.get_all_video_ids()
        video_ids = [vid for db_id, vid in all_videos]
        processor.close()
        log(f"Tìm thấy {len(video_ids)} videos trong database")
    
    if not video_ids:
        log("Không có video nào để xử lý", "ERROR")
        sys.exit(1)
    
    # Xử lý
    processor = YouTubeProcessor()
    
    try:
        results = processor.process_batch(video_ids)
        
        # Báo cáo kết quả
        success_count = sum(1 for v in results.values() if v)
        total_count = len(results)
        
        log(f"\n{'='*60}")
        log(f"HOÀN THÀNH: {success_count}/{total_count} videos thành công")
        log(f"{'='*60}")
        
        if success_count < total_count:
            failed = [vid for vid, ok in results.items() if not ok]
            log(f"Failed: {', '.join(failed)}", "WARNING")
        
        sys.exit(0 if success_count == total_count else 1)
        
    finally:
        processor.close()


if __name__ == "__main__":
    main()
