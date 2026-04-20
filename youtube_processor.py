#!/usr/bin/env python3
"""
YouTube Transcript Processor với Qwen AI (DashScope)
Lấy transcript từ YouTube, chia đoạn có ngữ cảnh, tạo quotes và bài đăng mạng xã hội
"""

import os
import sys
import re
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import mysql.connector
from mysql.connector import Error
import dashscope
from dashscope import Generation
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Cấu hình API Keys
DASHSCOPE_API_KEY = os.getenv('DASHSCOPE_API_KEY', '')
if not DASHSCOPE_API_KEY:
    logger.warning("⚠️  DASHSCOPE_API_KEY không được thiết lập. Vui lòng set biến môi trường.")
    logger.info("   Lấy API key miễn phí tại: https://dashscope.console.aliyun.com/")

dashscope.api_key = DASHSCOPE_API_KEY

# Cấu hình MySQL
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'your_database')
}


class MySQLConnection:
    """Quản lý kết nối MySQL"""
    
    def __init__(self):
        self.connection = None
    
    def connect(self):
        try:
            self.connection = mysql.connector.connect(**MYSQL_CONFIG)
            logger.info("✅ Kết nối MySQL thành công")
            return True
        except Error as e:
            logger.error(f"❌ Lỗi kết nối MySQL: {e}")
            return False
    
    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("🔌 Đã ngắt kết nối MySQL")
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = False):
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            
            if fetch:
                result = cursor.fetchall()
                cursor.close()
                return result
            
            self.connection.commit()
            cursor.close()
            return True
        except Error as e:
            logger.error(f"❌ Lỗi thực thi query: {e}")
            return False
    
    def get_video_by_id(self, video_id: str) -> Optional[Dict]:
        query = "SELECT * FROM youtube_videos WHERE video_id = %s"
        results = self.execute_query(query, (video_id,), fetch=True)
        return results[0] if results else None
    
    def get_pending_videos(self, limit: int = 10) -> List[Dict]:
        """Lấy các video chưa được xử lý"""
        query = """
            SELECT v.* FROM youtube_videos v
            LEFT JOIN youtube_paragraphs p ON v.id = p.youtube_video_id
            WHERE p.id IS NULL
            ORDER BY v.published_at DESC
            LIMIT %s
        """
        return self.execute_query(query, (limit,), fetch=True) or []
    
    def save_paragraph(self, video_id: int, ordinal: int, content_raw: str, content: str):
        query = """
            INSERT INTO youtube_paragraphs 
            (ordinal_number, content_raw, content, youtube_video_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, NOW(6), NOW(6))
        """
        return self.execute_query(query, (ordinal, content_raw, content, video_id))
    
    def save_quote(self, video_id: int, ordinal: int, content: str, is_visible: bool = True):
        query = """
            INSERT INTO youtube_quotes 
            (ordinal_number, content, is_visible, youtube_video_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, NOW(6), NOW(6))
        """
        return self.execute_query(query, (ordinal, content, 1 if is_visible else 0, video_id))
    
    def update_quote_visibility(self, quote_id: int, is_visible: bool):
        query = """
            UPDATE youtube_quotes 
            SET is_visible = %s, updated_at = NOW(6)
            WHERE id = %s
        """
        return self.execute_query(query, (1 if is_visible else 0, quote_id))


class TranscriptFetcher:
    """Lấy transcript từ YouTube"""
    
    @staticmethod
    def fetch(video_id: str) -> Optional[List[Dict]]:
        try:
            logger.info(f"📥 Đang lấy transcript cho video: {video_id}")
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            
            # Ưu tiên transcript tiếng Việt, sau đó là tiếng Anh
            try:
                transcript = transcript_list.find_transcript(['vi'])
                logger.info("✅ Tìm thấy transcript tiếng Việt")
            except:
                try:
                    transcript = transcript_list.find_transcript(['en'])
                    logger.info("✅ Tìm thấy transcript tiếng Anh")
                except:
                    # Lấy transcript đầu tiên có sẵn
                    transcript = transcript_list.find_generated_transcript(['vi', 'en'])
                    logger.info("✅ Sử dụng transcript auto-generated")
            
            data = transcript.fetch()
            logger.info(f"📊 Lấy được {len(data)} đoạn transcript")
            return data
        except TranscriptsDisabled:
            logger.error(f"❌ Transcript bị tắt cho video {video_id}")
            return None
        except NoTranscriptFound:
            logger.error(f"❌ Không tìm thấy transcript cho video {video_id}")
            return None
        except Exception as e:
            logger.error(f"❌ Lỗi khi lấy transcript: {e}")
            return None


class SegmentProcessor:
    """Xử lý chia đoạn transcript thành các paragraph có ngữ cảnh"""
    
    def __init__(self, min_gap_seconds: float = 1.5, min_words: int = 30, max_words: int = 200):
        self.min_gap_seconds = min_gap_seconds
        self.min_words = min_words
        self.max_words = max_words
    
    def process(self, transcript: List[Dict]) -> List[Dict]:
        """Chia transcript thành các đoạn có ý nghĩa"""
        if not transcript:
            return []
        
        paragraphs = []
        current_segment = []
        current_start = 0
        current_word_count = 0
        
        for i, entry in enumerate(transcript):
            text = entry.get('text', '').strip()
            start = entry.get('start', 0)
            duration = entry.get('duration', 0)
            
            if not text:
                continue
            
            # Kiểm tra khoảng cách thời gian để tách đoạn
            if current_segment:
                prev_entry = transcript[i-1] if i > 0 else None
                if prev_entry:
                    gap = start - (prev_entry.get('start', 0) + prev_entry.get('duration', 0))
                    
                    # Tách đoạn nếu khoảng cách đủ lớn và đã có đủ từ
                    if gap >= self.min_gap_seconds and current_word_count >= self.min_words:
                        # Tạo paragraph từ segment hiện tại
                        paragraph = self._create_paragraph(current_segment, current_start)
                        if paragraph:
                            paragraphs.append(paragraph)
                        
                        # Reset cho segment mới
                        current_segment = []
                        current_start = start
                        current_word_count = 0
            
            current_segment.append(entry)
            current_word_count += len(text.split())
            
            # Force split nếu quá dài
            if current_word_count >= self.max_words:
                paragraph = self._create_paragraph(current_segment, current_start)
                if paragraph:
                    paragraphs.append(paragraph)
                current_segment = []
                current_start = start
                current_word_count = 0
        
        # Xử lý segment cuối cùng
        if current_segment:
            paragraph = self._create_paragraph(current_segment, current_start)
            if paragraph:
                paragraphs.append(paragraph)
        
        logger.info(f"✂️  Chia thành {len(paragraphs)} đoạn")
        return paragraphs
    
    def _create_paragraph(self, segment: List[Dict], start_time: float) -> Optional[Dict]:
        """Tạo paragraph từ segment, đảm bảo không cắt ngang câu"""
        if not segment:
            return None
        
        # Ghép text
        full_text = ' '.join([entry.get('text', '').strip() for entry in segment])
        full_text = re.sub(r'\s+', ' ', full_text).strip()
        
        # Đảm bảo kết thúc ở cuối câu
        if full_text and not any(full_text.endswith(p) for p in '.!?…'):
            # Tìm dấu câu gần cuối nhất
            last_punct = -1
            for punct in '.!?…':
                pos = full_text.rfind(punct)
                if pos > last_punct:
                    last_punct = pos
            
            if last_punct > len(full_text) * 0.7:  # Chỉ cắt nếu dấu câu ở 30% cuối
                full_text = full_text[:last_punct + 1]
        
        if len(full_text.split()) < self.min_words:
            return None
        
        return {
            'content_raw': full_text,
            'start_time': start_time,
            'word_count': len(full_text.split())
        }


class QwenAIProcessor:
    """Sử dụng Qwen AI (DashScope) để tạo quotes và bài đăng"""
    
    def __init__(self, model: str = 'qwen-plus'):
        self.model = model
    
    def generate_quote(self, paragraph: str, context: str = "") -> Dict:
        """Tạo quote hay từ paragraph"""
        
        prompt = f"""Bạn là một chuyên gia trích dẫn sách và nội dung truyền cảm hứng. 
Nhiệm vụ: Chọn ra câu/trích dẫn HAY NHẤT và ĐÁNG NHỚ NHẤT từ đoạn văn dưới đây.

YÊU CẦU:
1. Quote phải đứng độc lập được, có ý nghĩa trọn vẹn
2. Độ dài: 1-3 câu (tối đa 50 từ)
3. Phải thể hiện được thông điệp chính hoặc điểm nhấn cảm xúc
4. Giữ nguyên văn phong gốc, không thêm bớt ý
5. Nếu có số liệu, tên riêng thì giữ lại

ĐOẠN VĂN GỐC:
{paragraph}

{f'NGỮ CẢNH (nếu có): {context}' if context else ''}

Hãy trả về JSON theo định dạng:
{{
    "quote": "nội dung trích dẫn",
    "reason": "lý do chọn quote này (1 câu)"
}}
"""
        
        try:
            response = Generation.call(
                model=self.model,
                prompt=prompt,
                result_format='message'
            )
            
            if response.status_code == 200:
                content = response.output.choices[0].message.content
                # Parse JSON từ response
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                    logger.info("✅ Tạo quote thành công")
                    return result
                else:
                    logger.warning("⚠️  Không parse được JSON, trả về quote thô")
                    return {"quote": content.strip(), "reason": "Auto-extracted"}
            else:
                logger.error(f"❌ Lỗi API Qwen: {response.code} - {response.message}")
                return {"quote": "", "reason": "API error"}
        except Exception as e:
            logger.error(f"❌ Lỗi khi gọi Qwen AI: {e}")
            return {"quote": "", "reason": f"Error: {str(e)}"}
    
    def generate_social_post(self, paragraph: str, quote: str, video_title: str, context_before: str = "", context_after: str = "") -> Dict:
        """Tạo bài đăng mạng xã hội tự nhiên như người thật chia sẻ"""
        
        prompt = f"""Bạn là một người yêu sách và muốn chia sẻ cảm xúc thật của mình về một đoạn trích hay vừa đọc được.
Viết một bài đăng mạng xã hội (Facebook/Instagram) TỰ NHIÊN, CHÂN THẬT và GÂY CẢM XÚC.

THÔNG TIN ĐẦU VÀO:
- Tên video/sách: {video_title}
- Đoạn trích chính: {paragraph}
- Quote đắt giá: {quote}
{f'- Ngữ cảnh trước: {context_before}' if context_before else ''}
{f'- Ngữ cảnh sau: {context_after}' if context_after else ''}

YÊU CẦU VIẾT BÀI:
1. MỞ ĐẦU: Bắt đầu bằng cảm xúc/câu hỏi/tình huống thực tế (tránh "Hôm nay tôi đọc...")
2. THÂN BÀI: 
   - Dẫn dắt vào đoạn trích một cách tự nhiên
   - Trích nguyên văn quote (dùng dấu ngoặc kép)
   - Chia sẻ cảm nhận CÁ NHÂN, không phân tích sáo rỗng
3. KẾT BÀI: Câu hỏi mở hoặc lời mời tương tác
4. GIỌNG ĐIỆU: 
   - Như đang nói chuyện với bạn bè
   - Có thể dùng từ cảm thán (Ôi, Thật là, Quá hay...)
   - Tránh ngôn ngữ học thuật, giáo điều
5. ĐỘ DÀI: 150-300 từ
6. HASHTAG: 3-5 hashtag liên quan

VÍ DỤ MẪU (tham khảo phong cách):
"Đọc đến đoạn này mà tự dưng thấy mắt cay cay... 
'[Quote]'
Có những sự thật đơn giản vậy mà ta mãi đi tìm ở đâu xa. 
Bạn đã bao giờ...?"

HÃY VIẾT BÀI ĐĂNG:"""

        try:
            response = Generation.call(
                model=self.model,
                prompt=prompt,
                result_format='message'
            )
            
            if response.status_code == 200:
                content = response.output.choices[0].message.content.strip()
                logger.info("✅ Tạo bài post thành công")
                
                # Đánh giá chất lượng bài post
                quality_score = self._evaluate_post(content, paragraph, quote)
                
                return {
                    "post": content,
                    "quality_score": quality_score,
                    "is_good": quality_score >= 6
                }
            else:
                logger.error(f"❌ Lỗi API Qwen: {response.code} - {response.message}")
                return {"post": "", "quality_score": 0, "is_good": False}
        except Exception as e:
            logger.error(f"❌ Lỗi khi gọi Qwen AI: {e}")
            return {"post": "", "quality_score": 0, "is_good": False}
    
    def _evaluate_post(self, post: str, paragraph: str, quote: str) -> int:
        """Đánh giá chất lượng bài post (0-10)"""
        score = 5  # Điểm cơ bản
        
        # Check độ dài
        word_count = len(post.split())
        if 150 <= word_count <= 300:
            score += 1
        elif word_count < 50:
            score -= 2
        
        # Check có quote không
        if quote in post or any(q_word in post for q_word in quote.split()[:5]):
            score += 1
        
        # Check có cảm xúc (từ cảm thán, câu hỏi)
        if any(word in post.lower() for word in ['!', '?', 'ôi', 'thật', 'quá', 'đúng là']):
            score += 1
        
        # Check có câu hỏi tương tác
        if '?' in post and post.strip().endswith('?'):
            score += 1
        
        # Check tránh sáo rỗng
        cliche_words = ['sâu sắc', 'ý nghĩa', 'đáng suy ngẫm', 'bài học quý']
        if any(word in post for word in cliche_words):
            score -= 1
        
        return max(0, min(10, score))
    
    def enhance_paragraph(self, paragraph: str, context: str = "") -> str:
        """Làm mượt paragraph nếu cần"""
        
        prompt = f"""Biên tập lại đoạn văn sau để đọc trôi chảy hơn nhưng GIỮ NGUYÊN Ý NGHĨA và NỘI DUNG:
- Sửa lỗi chính tả, ngữ pháp
- Thêm dấu câu hợp lý
- Không thêm/bớt thông tin
- Giữ nguyên giọng văn gốc

ĐOẠN VĂN:
{paragraph}

BIÊN TẬP LẠI:"""

        try:
            response = Generation.call(
                model=self.model,
                prompt=prompt,
                result_format='message'
            )
            
            if response.status_code == 200:
                content = response.output.choices[0].message.content.strip()
                return content if content else paragraph
            return paragraph
        except:
            return paragraph


def process_video(db: MySQLConnection, video_id: str, ai_processor: QwenAIProcessor) -> bool:
    """Xử lý một video hoàn chỉnh"""
    
    logger.info(f"\n{'='*60}")
    logger.info(f"🎬 Xử lý video: {video_id}")
    logger.info(f"{'='*60}")
    
    # 1. Lấy thông tin video từ DB
    video = db.get_video_by_id(video_id)
    if not video:
        logger.error(f"❌ Video {video_id} không tồn tại trong DB")
        return False
    
    video_title = video.get('title', 'Unknown')
    video_db_id = video.get('id')
    logger.info(f"📺 Title: {video_title}")
    
    # 2. Lấy transcript
    transcript = TranscriptFetcher.fetch(video_id)
    if not transcript:
        logger.warning("⚠️  Không có transcript, bỏ qua video này")
        return False
    
    # 3. Chia đoạn
    processor = SegmentProcessor(min_gap_seconds=1.5, min_words=30, max_words=200)
    paragraphs = processor.process(transcript)
    
    if not paragraphs:
        logger.warning("⚠️  Không chia được đoạn nào")
        return False
    
    # 4. Lưu và xử lý từng paragraph
    for idx, para in enumerate(paragraphs, 1):
        content_raw = para['content_raw']
        
        # Làm mượt paragraph
        content_enhanced = ai_processor.enhance_paragraph(content_raw)
        
        # Lưu paragraph
        db.save_paragraph(video_db_id, idx, content_raw, content_enhanced)
        logger.info(f"💾 Lưu paragraph {idx}/{len(paragraphs)}")
        
        # Tạo quote
        context = f"Video: {video_title}"
        quote_result = ai_processor.generate_quote(content_enhanced, context)
        
        if quote_result.get('quote'):
            db.save_quote(video_db_id, idx, quote_result['quote'], is_visible=True)
            logger.info(f"✨ Quote: {quote_result['quote'][:80]}...")
            
            # Tạo social post (chỉ với 3 paragraph đầu để tiết kiệm API)
            if idx <= 3:
                context_before = paragraphs[idx-2]['content_raw'] if idx > 1 else ""
                context_after = paragraphs[idx]['content_raw'] if idx < len(paragraphs) else ""
                
                post_result = ai_processor.generate_social_post(
                    content_enhanced,
                    quote_result['quote'],
                    video_title,
                    context_before,
                    context_after
                )
                
                if post_result.get('is_good'):
                    logger.info(f"📝 Post tốt (score: {post_result['quality_score']}/10)")
                    logger.info(f"   {post_result['post'][:100]}...")
                else:
                    logger.warning(f"⚠️  Post chưa tốt (score: {post_result['quality_score']}/10)")
        else:
            logger.warning("⚠️  Không tạo được quote cho paragraph này")
    
    logger.info(f"✅ Hoàn thành xử lý video {video_id}")
    return True


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='YouTube Transcript Processor với Qwen AI')
    parser.add_argument('video_id', nargs='?', help='YouTube Video ID')
    parser.add_argument('--file', type=str, help='File chứa danh sách video IDs')
    parser.add_argument('--all', action='store_true', help='Xử lý tất cả video chưa xử lý')
    parser.add_argument('--limit', type=int, default=10, help='Số video tối đa khi dùng --all')
    
    args = parser.parse_args()
    
    # Kết nối DB
    db = MySQLConnection()
    if not db.connect():
        sys.exit(1)
    
    # Khởi tạo AI processor
    ai_processor = QwenAIProcessor(model='qwen-plus')
    
    try:
        if args.video_id:
            process_video(db, args.video_id, ai_processor)
        elif args.file:
            with open(args.file, 'r') as f:
                video_ids = [line.strip() for line in f if line.strip()]
            logger.info(f"📋 Xử lý {len(video_ids)} video từ file")
            for vid in video_ids:
                process_video(db, vid, ai_processor)
        elif args.all:
            videos = db.get_pending_videos(limit=args.limit)
            if not videos:
                logger.info("✅ Không có video nào cần xử lý")
            else:
                logger.info(f"📋 Xử lý {len(videos)} video chưa xử lý")
                for video in videos:
                    process_video(db, video['video_id'], ai_processor)
        else:
            parser.print_help()
    finally:
        db.disconnect()


if __name__ == '__main__':
    main()
