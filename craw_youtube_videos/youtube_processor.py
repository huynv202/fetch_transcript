#!/usr/bin/env python3
"""
YouTube Transcript Processor - Phiên bản Python thuần
Lấy transcript từ YouTube, chia đoạn có ngữ cảnh, tạo quote và caption bằng AI
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
from typing import Optional, List, Dict, Any
import mysql.connector
from mysql.connector import Error
from openai import OpenAI
import requests

# ============================================================================
# CẤU HÌNH
# ============================================================================

SKIP_TEXTS = {"[music]", "[âm nhạc]", "[nhạc]", "[tiếng nhạc]", "♪", "♫"}
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
GEMINI_API_KEYS = [
    "AIzaSyBIsSyFNutubWm9_ANjXBM4l9eN1_wIhio",
]

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


# ============================================================================
# MYSQL CONNECTION
# ============================================================================

def get_mysql_connection():
    """Tạo kết nối MySQL"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        return conn
    except Error as e:
        print(f"❌ Lỗi kết nối MySQL: {e}")
        return None


def fetch_video_by_id(conn, video_id: str) -> Optional[Dict]:
    """Lấy thông tin video từ DB"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM youtube_videos WHERE video_id = %s", (video_id,))
    result = cursor.fetchone()
    cursor.close()
    return result


def fetch_videos_without_quotes(conn, limit: int = 100) -> List[Dict]:
    """Lấy danh sách video chưa có quotes"""
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT v.* FROM youtube_videos v
        LEFT JOIN youtube_quotes q ON v.id = q.youtube_video_id
        WHERE q.id IS NULL AND v.video_id IS NOT NULL
        LIMIT %s
    """
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    cursor.close()
    return results


def delete_existing_paragraphs(conn, video_db_id: int):
    """Xóa paragraphs cũ của video"""
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM youtube_paragraphs WHERE youtube_video_id = %s",
        (video_db_id,)
    )
    conn.commit()
    cursor.close()


def delete_existing_quotes(conn, video_db_id: int):
    """Xóa quotes cũ của video"""
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM youtube_quotes WHERE youtube_video_id = %s",
        (video_db_id,)
    )
    conn.commit()
    cursor.close()


def save_paragraph(conn, video_db_id: int, ordinal: int, content_raw: str, content: str):
    """Lưu paragraph vào DB"""
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO youtube_paragraphs 
           (youtube_video_id, ordinal_number, content_raw, content, created_at, updated_at)
           VALUES (%s, %s, %s, %s, NOW(6), NOW(6))""",
        (video_db_id, ordinal, content_raw, content)
    )
    conn.commit()
    cursor.close()


def save_quote(conn, video_db_id: int, ordinal: int, content: str, is_visible: bool = True):
    """Lưu quote vào DB"""
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO youtube_quotes 
           (youtube_video_id, ordinal_number, content, is_visible, created_at, updated_at)
           VALUES (%s, %s, %s, %s, NOW(6), NOW(6))""",
        (video_db_id, ordinal, content, is_visible)
    )
    conn.commit()
    cursor.close()


# ============================================================================
# TRANSCRIPT FETCHING (giữ nguyên logic từ fetch_transcript.py)
# ============================================================================

def build_proxy_config():
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    if not http_proxy and not https_proxy:
        return None
    return {"http": http_proxy, "https": https_proxy}


def build_http_client():
    session = requests.Session()
    session.headers.update({"Accept-Language": "en-US,en;q=0.9,vi;q=0.8"})
    
    cookie_file = os.getenv("YOUTUBE_COOKIE_FILE")
    if cookie_file and os.path.exists(cookie_file):
        cookie_jar = MozillaCookieJar()
        cookie_jar.load(cookie_file, ignore_discard=True, ignore_expires=True)
        session.cookies = cookie_jar
    
    return session


def normalize_segments(items: List[Dict], language: str, video_id: str = None) -> Dict:
    segments = []
    full_text = []

    for item in items:
        text = item.get("text", "").strip()
        if not text or text.lower() in SKIP_TEXTS:
            continue
        
        segments.append({
            "start": float(item.get("start", 0)),
            "duration": float(item.get("duration", 0)),
            "text": text
        })
        full_text.append(text)

    return {
        "video_id": video_id,
        "language": language,
        "segments": segments,
        "transcript": "\n".join(full_text)
    }


def fetch_url(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9,vi;q=0.8"
        }
    )
    proxy_handler = urllib.request.ProxyHandler(build_proxy_config() or {})
    handlers = [proxy_handler]

    cookie_file = os.getenv("YOUTUBE_COOKIE_FILE")
    if cookie_file and os.path.exists(cookie_file):
        cookie_jar = MozillaCookieJar()
        cookie_jar.load(cookie_file, ignore_discard=True, ignore_expires=True)
        handlers.append(urllib.request.HTTPCookieProcessor(cookie_jar))

    opener = urllib.request.build_opener(*handlers)
    with opener.open(request, timeout=20) as response:
        return response.read().decode("utf-8", errors="ignore")


def extract_caption_tracks(video_id: str) -> List[Dict]:
    watch_html = fetch_url(f"https://www.youtube.com/watch?v={video_id}")
    match = re.search(r'"captionTracks":(\[.*?\])', watch_html)
    if not match:
        raise ValueError("No caption tracks found on watch page")
    return json.loads(html.unescape(match.group(1)))


def pick_caption_track(caption_tracks: List[Dict], langs: List[str]) -> Optional[Dict]:
    def track_matches(track, lang):
        track_lang = (track.get("languageCode") or "").lower()
        return track_lang == lang or track_lang.startswith(f"{lang}-")

    for lang in langs:
        for track in caption_tracks:
            if track_matches(track, lang) and track.get("kind") != "asr":
                return track

    for lang in langs:
        for track in caption_tracks:
            if track_matches(track, lang):
                return track

    return caption_tracks[0] if caption_tracks else None


def parse_caption_xml(xml_text: str) -> List[Dict]:
    root = ET.fromstring(xml_text)
    items = []
    for node in root.findall(".//text"):
        text = "".join(node.itertext()).strip()
        items.append({
            "start": node.attrib.get("start", 0),
            "duration": node.attrib.get("dur", 0),
            "text": html.unescape(text)
        })
    return items


def parse_caption_json(json_text: str) -> List[Dict]:
    payload = json.loads(json_text)
    items = []
    for event in payload.get("events", []):
        if "segs" not in event:
            continue
        text = "".join(seg.get("utf8", "") for seg in event["segs"]).strip()
        items.append({
            "start": event.get("tStartMs", 0) / 1000,
            "duration": event.get("dDurationMs", 0) / 1000,
            "text": html.unescape(text)
        })
    return items


def fetch_transcript(video_id: str, lang: str = "vi") -> Dict:
    """Fetch transcript từ YouTube"""
    from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
    from youtube_transcript_api.proxies import GenericProxyConfig

    langs = [lang] + [item for item in DEFAULT_LANGS if item != lang]
    errors = []

    # Thử dùng youtube-transcript-api trước
    try:
        proxy_config = None
        proxy_dict = build_proxy_config()
        if proxy_dict:
            proxy_config = GenericProxyConfig(
                http_url=proxy_dict.get("http"),
                https_url=proxy_dict.get("https")
            )
        
        ytt_api = YouTubeTranscriptApi(
            proxy_config=proxy_config,
            http_client=build_http_client()
        )
        transcript_list = ytt_api.list(video_id)
        
        try:
            transcript = transcript_list.find_transcript(langs)
        except Exception:
            transcript = transcript_list.find_generated_transcript(langs)
        
        data = transcript.fetch()
        result = normalize_segments([
            {"start": item.start, "duration": item.duration, "text": item.text}
            for item in data
        ], transcript.language_code, video_id)
        return result
    except TranscriptsDisabled:
        errors.append("Transcripts are disabled for this video")
    except NoTranscriptFound:
        errors.append("No transcript found")
    except Exception as e:
        errors.append(f"youtube-transcript-api failed: {e}")

    # Fallback: lấy từ caption track
    try:
        caption_tracks = extract_caption_tracks(video_id)
        track = pick_caption_track(caption_tracks, langs)
        if not track:
            raise ValueError("No usable caption track found")

        base_url = track["baseUrl"]
        try:
            parsed_url = urllib.parse.urlparse(base_url)
            query = urllib.parse.parse_qs(parsed_url.query)
            query["fmt"] = ["json3"]
            json3_url = urllib.parse.urlunparse(
                parsed_url._replace(query=urllib.parse.urlencode(query, doseq=True))
            )
            items = parse_caption_json(fetch_url(json3_url))
        except Exception:
            items = parse_caption_xml(fetch_url(base_url))

        result = normalize_segments(items, track.get("languageCode", langs[0]), video_id)
        return result
    except Exception as e:
        errors.append(f"Caption track fallback failed: {e}")

    return {"error": "\n".join(errors)}


# ============================================================================
# SMART PARAGRAPH SPLITTING (CẢI TIẾN)
# ============================================================================

def is_sentence_ending(text: str) -> bool:
    """Kiểm tra xem text có kết thúc bằng dấu câu hoàn chỉnh không"""
    text = text.strip()
    if not text:
        return False
    # Kết thúc bằng . ! ? ... : và có thể có dấu ngoặc đóng
    return bool(re.search(r'[.!?…:]["\'\)]?\s*$', text))


def find_safe_split_point(text: str, max_bytes: int) -> int:
    """Tìm điểm cắt an toàn (không cắt ngang câu)"""
    if len(text.encode('utf-8')) <= max_bytes:
        return len(text)
    
    # Cắt ở max_bytes
    candidate = text[:max_bytes]
    
    # Đảm bảo không cắt ngang ký tự UTF-8
    while candidate and not candidate.encode('utf-8').decode('utf-8', errors='ignore'):
        candidate = candidate[:-1]
    
    # Tìm dấu câu gần nhất để cắt
    last_period = candidate.rfind('.')
    last_exclaim = candidate.rfind('!')
    last_question = candidate.rfind('?')
    last_newline = candidate.rfind('\n')
    
    split_points = [p for p in [last_period, last_exclaim, last_question, last_newline] if p > 0]
    
    if split_points:
        # Chọn điểm cắt xa nhất nhưng vẫn trong giới hạn
        return max(split_points) + 1
    
    # Nếu không tìm thấy, cắt ở khoảng trắng cuối cùng
    last_space = candidate.rfind(' ')
    if last_space > max_bytes // 2:
        return last_space
    
    return max_bytes


def split_long_text(text: str, max_bytes: int) -> List[str]:
    """Chia text dài thành nhiều đoạn nhỏ, đảm bảo không cắt ngang câu"""
    if not text or len(text.encode('utf-8')) <= max_bytes:
        return [text] if text else []
    
    paragraphs = []
    remaining = text.strip()
    
    while remaining:
        if len(remaining.encode('utf-8')) <= max_bytes:
            paragraphs.append(remaining.strip())
            break
        
        split_idx = find_safe_split_point(remaining, max_bytes)
        segment = remaining[:split_idx].strip()
        
        if segment:
            paragraphs.append(segment)
        
        remaining = remaining[split_idx:].lstrip()
        
        # Tránh vòng lặp vô tận
        if split_idx == 0:
            paragraphs.append(remaining[:max_bytes].strip())
            remaining = remaining[max_bytes:].lstrip()
    
    return paragraphs


def build_paragraphs(segments: List[Dict], config: Dict = None) -> List[str]:
    """
    Xây dựng các paragraph từ segments với logic thông minh:
    - Giữ ngữ cảnh giữa các câu
    - Không cắt ngang câu đang nói
    - Tôn trọng khoảng nghỉ tự nhiên
    """
    config = config or PARAGRAPH_CONFIG
    paragraphs = []
    chunk_segments = []
    
    # Lọc và chuẩn hóa segments
    normalized = []
    for seg in segments:
        text = seg.get("text", "").strip()
        if not text:
            continue
        normalized.append({
            "text": text,
            "start": float(seg.get("start", 0)),
            "duration": float(seg.get("duration", 0))
        })
    
    if not normalized:
        return []
    
    for i, segment in enumerate(normalized):
        chunk_segments.append(segment)
        next_segment = normalized[i + 1] if i + 1 < len(normalized) else None
        
        # Kiểm tra xem có nên flush paragraph không
        current_text = " ".join(s["text"] for s in chunk_segments)
        current_bytes = len(current_text.encode('utf-8'))
        
        should_flush = False
        
        # Vượt quá max_bytes → buộc phải flush
        if current_bytes >= config["max_bytes"]:
            should_flush = True
        
        # Đạt target_bytes và có điều kiện thuận lợi
        elif current_bytes >= config["target_bytes"]:
            # Có đủ segments
            if len(chunk_segments) >= config["max_segments"]:
                should_flush = True
            # Kết thúc tự nhiên (dấu câu)
            elif is_sentence_ending(current_text):
                should_flush = True
            # Có khoảng nghỉ lớn
            elif next_segment and has_time_gap(segment, next_segment, config["break_gap_seconds"]):
                should_flush = True
        
        if should_flush and current_bytes >= config["min_bytes"]:
            # Extract và thêm vào paragraphs
            text = " ".join(s["text"] for s in chunk_segments).strip()
            if len(text.encode('utf-8')) > config["max_bytes"]:
                # Chia nhỏ nếu quá dài
                sub_paragraphs = split_long_text(text, config["max_bytes"])
                paragraphs.extend(sub_paragraphs)
            else:
                paragraphs.append(text)
            chunk_segments = []
    
    # Xử lý phần còn lại
    if chunk_segments:
        text = " ".join(s["text"] for s in chunk_segments).strip()
        if text:
            if len(text.encode('utf-8')) > config["max_bytes"]:
                sub_paragraphs = split_long_text(text, config["max_bytes"])
                paragraphs.extend(sub_paragraphs)
            else:
                paragraphs.append(text)
    
    return paragraphs


def has_time_gap(current_seg: Dict, next_seg: Dict, gap_seconds: float) -> bool:
    """Kiểm tra xem có khoảng nghỉ thời gian giữa 2 segments không"""
    if not next_seg:
        return True
    current_end = current_seg["start"] + current_seg["duration"]
    next_start = next_seg["start"]
    return (next_start - current_end) >= gap_seconds


# ============================================================================
# AI QUOTE GENERATION (CẢI TIẾN ĐÁNG KỂ)
# ============================================================================

def build_enhanced_prompt(transcript_chunk: str, video_title: str = "", book_name: str = "") -> str:
    """
    Xây dựng prompt cải tiến với đầy đủ ngữ cảnh
    """
    # Lấy ngữ cảnh xung quanh (nếu có)
    context_hint = ""
    if video_title:
        context_hint += f"- Video này có tiêu đề: \"{video_title}\"\n"
    if book_name:
        context_hint += f"- Nội dung liên quan đến sách: \"{book_name}\"\n"
    
    return f"""Bạn là một người yêu sách và muốn chia sẻ những trích dẫn hay từ video đọc sách trên YouTube.

THÔNG TIN NGỮ CẢNH:
{context_hint if context_hint else "- Không có thông tin bổ sung"}

ĐOẠN TRANSCRIPT CẦN XỬ LÝ:
\"\"\"
{transcript_chunk}
\"\"\"

NHIỆM VỤ:
Từ đoạn transcript trên, hãy tạo MỘT bài đăng ngắn gọn, tự nhiên và cảm xúc để chia sẻ trên mạng xã hội.

YÊU CẦU CHI TIẾT:

1. PHONG CÁCH VIẾT:
   - Viết như một người thật vừa đọc xong và muốn chia sẻ cảm xúc
   - Tự nhiên, chân thành, không máy móc hay công thức
   - Có thể bắt đầu bằng cảm nhận cá nhân: "Đọc tới đây mình thấy...", "Điều này làm mình suy nghĩ...", "Thật sự ấn tượng với..."

2. CẤU TRÚ BÀI ĐĂNG (ưu tiên theo thứ tự):
   
   Option A (Tốt nhất - Nếu có câu trích hay):
   - 1 câu cảm nhận ngắn (10-20 từ)
   - 1 câu trích NGUYÊN VĂN từ transcript (15-40 từ) đặt trong ngoặc kép
   - Ví dụ: "Đọc tới đây mình thấm thía rằng đôi khi điều ta cần nhất chỉ là được thấu hiểu: \"Ai cũng khao khát được lắng nghe và trân trọng.\""
   
   Option B (Nếu không có câu trích đủ hay):
   - 1-2 câu cảm nhận rõ ràng, có ý nghĩa
   - KHÔNG cố chèn ngoặc kép nếu không có gì đáng trích
   - Ví dụ: "Thông điệp này nhắc nhở mình rằng trong cuộc sống vội vã, đôi khi ta quên mất việc dừng lại để lắng nghe chính mình."

3. TIÊU CHÍ CHỌN CÂU TRÍCH (cho Option A):
   - Phải là câu HOÀN CHỈNH, đủ chủ ngữ vị ngữ
   - Có ý nghĩa độc lập, không cần nhiều ngữ cảnh vẫn hiểu
   - Gợi cảm xúc hoặc suy ngẫm
   - Độ dài: 15-40 từ (ưu tiên 20-30 từ)
   - KHÔNG chọn: câu quá ngắn (<8 từ), câu cụt, câu cần ngữ cảnh mới hiểu, câu chung chung nhạt nhẽo

4. NHỮNG ĐIỀU CẦN TRÁNH:
   - ❌ Không bịa đặt câu trích không có trong transcript
   - ❌ Không thêm tên sách nếu không chắc chắn
   - ❌ Không viết quá 2 câu tổng cộng
   - ❌ Không giải thích dài dòng, không meta-comment ("Đây là câu nói hay...")
   - ❌ Không dùng ngôn ngữ quá trang trọng hay học thuật
   - ❌ Không cắt ngang câu đang nói để làm quote

5. ĐỊNH DẠNG ĐẦU RA:
   - Chỉ trả về NỘI DUNG bài đăng cuối cùng
   - KHÔNG thêm giải thích, KHÔNG thêm markdown, KHÔNG thêm "Output:" hay prefix nào
   - Nếu có câu trích, đặt trong ngoặc kép "..."

VÍ DỤ ĐẦU RA TỐT:
- "Đọc đoạn này mình nhận ra hạnh phúc đôi khi đơn giản lắm: \"Hạnh phúc không phải là có tất cả, mà là biết trân trọng những gì đang có.\""
- "Câu này làm mình suy nghĩ nhiều về cách ta đối xử với nhau: \"Lời nói có thể làm tổn thương, nhưng cũng có thể chữa lành.\""
- "Thông điệp này nhắc nhở mình sống chậm lại một chút: \"Cuộc đời không chờ đợi ai, nhưng cũng không bao giờ là quá muộn để bắt đầu.\""
- "Thật sự ấn tượng với cách tác giả diễn đạt: \"Chúng ta thường sợ thất bại, nhưng thực ra sợ nhất là không bao giờ thử.\""

Bây giờ, hãy viết bài đăng dựa trên đoạn transcript đã cho:"""


def generate_with_gemini(text: str, video_title: str = "") -> Optional[str]:
    """Sinh quote/caption bằng Gemini API"""
    api_key = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else None
    if not api_key:
        print("⚠️ Thiếu Gemini API key")
        return None
    
    url = f"https://generativelanguage.googleapis.com/v1/models/gemini-2.0-flash:generateContent?key={api_key}"
    
    prompt = build_enhanced_prompt(text, video_title)
    
    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }]
    }
    
    try:
        response = requests.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if not data.get("candidates"):
            print(f"⚠️ Gemini không trả về candidates: {data}")
            return None
        
        result = data["candidates"][0]["content"]["parts"][0]["text"]
        return result.strip()
    
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Gemini API lỗi: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"⚠️ Lỗi parse response Gemini: {e}")
        return None


def generate_with_openai(text: str, video_title: str = "") -> Optional[str]:
    """Sinh quote/caption bằng OpenAI API (fallback)"""
    if not OPENAI_API_KEY:
        return None
    
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = build_enhanced_prompt(text, video_title)
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=200
        )
        
        result = response.choices[0].message.content
        return result.strip() if result else None
    
    except Exception as e:
        print(f"⚠️ OpenAI API lỗi: {e}")
        return None


def normalize_and_validate_quote(ai_output: str, original_text: str) -> Optional[str]:
    """
    Chuẩn hóa và validate quote từ AI:
    - Làm sạch
    - Kiểm tra độ dài
    - Đảm bảo có ý nghĩa
    """
    if not ai_output:
        return None
    
    # Làm sạch
    result = ai_output.strip()
    
    # Loại bỏ các prefix không mong muốn
    prefixes_to_remove = [
        "Output:", "Kết quả:", "Bài đăng:", "Caption:", 
        "Đây là:", "Result:", "Here's:"
    ]
    for prefix in prefixes_to_remove:
        if result.lower().startswith(prefix.lower()):
            result = result[len(prefix):].strip()
    
    # Loại bỏ markdown quotes
    if result.startswith("> "):
        result = result[2:]
    
    # Loại bỏ dấu ngoặc kép thừa ở đầu/cuối nếu toàn bộ là quote
    if result.startswith('"') and result.endswith('"') and result.count('"') == 2:
        # Đây có thể là trường hợp AI chỉ trả về câu trích, thêm cảm nhận
        # Nhưng ta giữ nguyên vì có thể user muốn vậy
        pass
    
    # Kiểm tra độ dài tối thiểu
    if len(result) < 30:
        print(f"⚠️ Quote quá ngắn ({len(result)} chars): {result[:50]}...")
        return None
    
    # Kiểm tra độ dài tối đa (khoảng 2 câu)
    sentences = re.split(r'[.!?…]+', result)
    sentences = [s.strip() for s in sentences if s.strip()]
    if len(sentences) > 3:
        print(f"⚠️ Quote quá dài ({len(sentences)} câu)")
        # Cắt lấy 2 câu đầu
        result = ". ".join(sentences[:2]) + "."
    
    # Nếu có ngoặc kép, kiểm tra câu trích bên trong
    quoted_texts = re.findall(r'"([^"]+)"', result)
    if quoted_texts:
        longest_quote = max(quoted_texts, key=len).strip()
        words = longest_quote.split()
        
        # Quote quá ngắn
        if len(words) < 8:
            print(f"⚠️ Câu trích quá ngắn ({len(words)} từ): {longest_quote}")
            # Vẫn giữ kết quả nhưng log warning
        # Quote quá dài
        elif len(words) > 50:
            print(f"⚠️ Câu trích quá dài ({len(words)} từ)")
    
    return result if result else None


def evaluate_quote_quality(quote: str, original_text: str) -> float:
    """
    Đánh giá chất lượng quote (0-10)
    Trả về score để lọc quote kém chất lượng
    """
    score = 5.0  # Điểm cơ bản
    
    # Có câu trích trong ngoặc kép
    if '"' in quote:
        quoted = re.findall(r'"([^"]+)"', quote)
        if quoted:
            longest = max(quoted, key=len)
            words = longest.split()
            
            # Độ dài câu trích lý tưởng
            if 15 <= len(words) <= 35:
                score += 2
            elif 8 <= len(words) < 15 or 35 < len(words) <= 50:
                score += 1
            
            # Câu trích có vẻ hoàn chỉnh
            if re.search(r'[.!?]$', longest):
                score += 1
    
    # Có cảm nhận cá nhân
    personal_phrases = ["mình thấy", "mình nhận ra", "làm mình", "ấn tượng", "thấm thía", "nhớ lại"]
    if any(p in quote.lower() for p in personal_phrases):
        score += 1.5
    
    # Độ dài phù hợp
    total_len = len(quote)
    if 50 <= total_len <= 300:
        score += 1
    elif total_len < 50:
        score -= 2
    elif total_len > 400:
        score -= 1
    
    # Không có từ ngữ tiêu cực mạnh
    negative_words = ["chán", "tệ", "dở", "vô dụng", "ngu ngốc"]
    if any(w in quote.lower() for w in negative_words):
        score -= 1
    
    return max(0, min(10, score))


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_video(conn, video: Dict, verbose: bool = True) -> bool:
    """
    Xử lý một video:
    1. Lấy transcript
    2. Chia paragraphs
    3. Tạo quotes với AI
    4. Lưu vào DB
    """
    video_id = video["video_id"]
    video_db_id = video["id"]
    video_title = video.get("title", "")
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"🎬 Xử lý video: {video_id}")
        if video_title:
            print(f"   Tiêu đề: {video_title}")
    
    # Bước 1: Fetch transcript
    if verbose:
        print("📥 Đang lấy transcript...")
    
    transcript_data = fetch_transcript(video_id, lang="vi")
    
    if "error" in transcript_data:
        print(f"❌ Lỗi lấy transcript: {transcript_data['error']}")
        return False
    
    segments = transcript_data.get("segments", [])
    if not segments:
        print("❌ Không có segments nào")
        return False
    
    if verbose:
        print(f"✅ Lấy được {len(segments)} segments")
    
    # Bước 2: Build paragraphs
    if verbose:
        print("📝 Đang chia paragraphs...")
    
    # Xóa paragraphs cũ
    delete_existing_paragraphs(conn, video_db_id)
    
    paragraphs = build_paragraphs(segments)
    
    if not paragraphs:
        print("❌ Không tạo được paragraphs")
        return False
    
    # Lưu paragraphs
    for i, para in enumerate(paragraphs, 1):
        save_paragraph(conn, video_db_id, i, para, para)
    
    if verbose:
        print(f"✅ Đã lưu {len(paragraphs)} paragraphs")
    
    # Bước 3: Generate quotes với AI
    if verbose:
        print("🤖 Đang tạo quotes với AI...")
    
    # Xóa quotes cũ
    delete_existing_quotes(conn, video_db_id)
    
    # Chia segments thành nhóm nhỏ để tạo quotes
    # Mỗi nhóm ~5 segments để có đủ ngữ cảnh
    groups = [segments[i:i+5] for i in range(0, len(segments), 5)]
    
    quotes_created = 0
    quotes_skipped = 0
    
    for i, group in enumerate(groups):
        raw_text = " ".join(s["text"] for s in group)
        
        # Skip nếu quá ngắn
        if len(raw_text.encode('utf-8')) < 200:
            quotes_skipped += 1
            continue
        
        # Thử Gemini trước
        ai_output = generate_with_gemini(raw_text, video_title)
        
        # Fallback sang OpenAI nếu cần
        if not ai_output and OPENAI_API_KEY:
            if verbose:
                print(f"   ⚡ Fallback sang OpenAI cho group {i+1}...")
            ai_output = generate_with_openai(raw_text, video_title)
        
        if not ai_output:
            quotes_skipped += 1
            continue
        
        # Chuẩn hóa và validate
        normalized = normalize_and_validate_quote(ai_output, raw_text)
        
        if not normalized:
            quotes_skipped += 1
            continue
        
        # Đánh giá chất lượng
        quality_score = evaluate_quote_quality(normalized, raw_text)
        is_visible = quality_score >= 6.0
        
        if verbose:
            visibility = "✅" if is_visible else "⚠️"
            print(f"   {visibility} Group {i+1}: score={quality_score:.1f}")
            if not is_visible and verbose:
                print(f"      Content: {normalized[:100]}...")
        
        # Lưu quote
        save_quote(conn, video_db_id, i + 1, normalized, is_visible)
        quotes_created += 1
    
    if verbose:
        print(f"✅ Đã tạo {quotes_created} quotes ({quotes_skipped} skipped)")
    
    return True


def main():
    """Hàm main"""
    print("="*60)
    print("🎬 YOUTUBE TRANSCRIPT PROCESSOR")
    print("="*60)
    
    # Kết nối MySQL
    conn = get_mysql_connection()
    if not conn:
        print("❌ Không thể kết nối MySQL. Kiểm tra cấu hình.")
        sys.exit(1)
    
    print("✅ Kết nối MySQL thành công")
    
    # Kiểm tra mode chạy
    if len(sys.argv) > 1:
        # Chế độ xử lý video cụ thể từ file ids.txt hoặc video_id truyền vào
        arg = sys.argv[1]
        
        if arg == "--file" and len(sys.argv) > 2:
            # Đọc từ file
            ids_file = sys.argv[2]
            if not os.path.exists(ids_file):
                print(f"❌ File không tồn tại: {ids_file}")
                sys.exit(1)
            
            with open(ids_file, 'r') as f:
                video_ids = [line.strip() for line in f if line.strip()]
            
            print(f"📄 Đọc được {len(video_ids)} video IDs từ {ids_file}")
            
            # Fetch info từ DB hoặc tạo mới
            videos_to_process = []
            for vid in video_ids:
                video = fetch_video_by_id(conn, vid)
                if video:
                    videos_to_process.append(video)
                else:
                    print(f"⚠️ Video {vid} không có trong DB, bỏ qua")
            
            if not videos_to_process:
                print("❌ Không có video nào để xử lý")
                sys.exit(1)
        
        elif arg == "--all":
            # Xử lý tất cả video chưa có quotes
            limit = int(sys.argv[3]) if len(sys.argv) > 3 else 100
            videos_to_process = fetch_videos_without_quotes(conn, limit)
            print(f"📊 Tìm được {len(videos_to_process)} video chưa có quotes")
        
        else:
            # Xử lý video_id cụ thể
            video_id = arg
            video = fetch_video_by_id(conn, video_id)
            if not video:
                print(f"❌ Video {video_id} không có trong DB")
                sys.exit(1)
            videos_to_process = [video]
    else:
        # Mặc định: xử lý video chưa có quotes
        videos_to_process = fetch_videos_without_quotes(conn, 50)
        print(f"📊 Tìm được {len(videos_to_process)} video chưa có quotes")
    
    if not videos_to_process:
        print("✅ Không có video nào cần xử lý")
        conn.close()
        sys.exit(0)
    
    # Xử lý từng video
    success_count = 0
    error_count = 0
    
    for video in videos_to_process:
        try:
            if process_video(conn, video):
                success_count += 1
            else:
                error_count += 1
        except Exception as e:
            print(f"❌ Lỗi xử lý video {video['video_id']}: {e}")
            error_count += 1
    
    # Tổng kết
    print("\n" + "="*60)
    print("📊 KẾT QUẢ:")
    print(f"   ✅ Thành công: {success_count}")
    print(f"   ❌ Thất bại: {error_count}")
    print("="*60)
    
    conn.close()
    sys.exit(0 if error_count == 0 else 1)


if __name__ == "__main__":
    main()
