import dotenv from 'dotenv';
dotenv.config();

import mysql from 'mysql2/promise';
import axios from 'axios';
import { YoutubeTranscript } from 'youtube-transcript/dist/youtube-transcript.esm.js';

// Cấu hình
const DB_CONFIG = {
  host: process.env.MYSQL_HOST || 'localhost',
  port: parseInt(process.env.MYSQL_PORT) || 3306,
  user: process.env.MYSQL_USER || 'root',
  password: process.env.MYSQL_PASSWORD || '',
  database: process.env.MYSQL_DATABASE || 'treebook'
};

const DASHSCOPE_API_KEY = process.env.DASHSCOPE_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

class YouTubeProcessor {
  constructor() {
    this.connection = null;
  }

  async connectDB() {
    try {
      console.log(`[INFO] Đang kết nối MySQL tại ${DB_CONFIG.host}:${DB_CONFIG.port}...`);
      this.connection = await mysql.createConnection(DB_CONFIG);
      console.log('[INFO] Kết nối MySQL thành công!');
    } catch (error) {
      console.error(`[ERROR] Lỗi kết nối MySQL: ${error.message}`);
      throw error;
    }
  }

  async disconnectDB() {
    if (this.connection) {
      await this.connection.end();
      console.log('[INFO] Đã ngắt kết nối MySQL.');
    }
  }

  async getVideoId(videoIdentifier) {
    const [rows] = await this.connection.execute(
      'SELECT id FROM youtube_videos WHERE video_id = ?',
      [videoIdentifier]
    );
    if (rows.length === 0) {
      throw new Error(`Không tìm thấy video với ID: ${videoIdentifier} trong bảng youtube_videos`);
    }
    return rows[0].id;
  }

  async fetchTranscript(videoId) {
    try {
      console.log(`[INFO] Đang lấy transcript cho video: ${videoId}...`);
      const transcriptItems = await YoutubeTranscript.fetchTranscript(videoId);
      
      if (!transcriptItems || transcriptItems.length === 0) {
        throw new Error('Không tìm thấy transcript cho video này.');
      }
      
      console.log(`[INFO] Đã lấy được ${transcriptItems.length} đoạn transcript.`);
      return transcriptItems;
    } catch (error) {
      console.error(`[ERROR] Lỗi khi lấy transcript: ${error.message}`);
      throw error;
    }
  }

  mergeSegments(transcriptItems) {
    const merged = [];
    let currentSegment = { text: '', start: 0, duration: 0 };

    for (let i = 0; i < transcriptItems.length; i++) {
      const item = transcriptItems[i];
      const text = (item.text || '').trim();
      
      if (!text) continue;

      if (currentSegment.text === '') {
        currentSegment.text = text;
        currentSegment.start = item.offset / 1000;
        currentSegment.duration = item.duration / 1000;
      } else {
        const gap = (item.offset / 1000) - (currentSegment.start + currentSegment.duration);
        
        if (gap < 1.5) {
          currentSegment.text += ' ' + text;
          currentSegment.duration += gap + (item.duration / 1000);
        } else {
          if (currentSegment.text.trim()) {
            merged.push({ ...currentSegment });
          }
          currentSegment = {
            text: text,
            start: item.offset / 1000,
            duration: item.duration / 1000
          };
        }
      }
    }

    if (currentSegment.text.trim()) {
      merged.push(currentSegment);
    }

    console.log(`[INFO] Đã gộp thành ${merged.length} đoạn có ý nghĩa.`);
    return merged;
  }

  splitIntoParagraphs(mergedSegments) {
    const paragraphs = [];
    let currentParagraph = { text: '', start: 0, duration: 0, segments: [] };

    for (const segment of mergedSegments) {
      if (currentParagraph.text === '') {
        currentParagraph.text = segment.text;
        currentParagraph.start = segment.start;
        currentParagraph.duration = segment.duration;
        currentParagraph.segments = [segment];
      } else {
        const lastChar = currentParagraph.text.slice(-1);
        const isSentenceEnd = ['.', '!', '?', '。', '!', '?'].includes(lastChar);
        const isLongEnough = currentParagraph.text.length > 150;

        if (isSentenceEnd && isLongEnough) {
          paragraphs.push({
            text: currentParagraph.text.trim(),
            start: currentParagraph.start,
            duration: currentParagraph.duration,
            content_raw: currentParagraph.segments.map(s => s.text).join(' ')
          });
          
          currentParagraph = {
            text: segment.text,
            start: segment.start,
            duration: segment.duration,
            segments: [segment]
          };
        } else {
          currentParagraph.text += ' ' + segment.text;
          currentParagraph.duration += segment.duration;
          currentParagraph.segments.push(segment);
        }
      }
    }

    if (currentParagraph.text.trim()) {
      paragraphs.push({
        text: currentParagraph.text.trim(),
        start: currentParagraph.start,
        duration: currentParagraph.duration,
        content_raw: currentParagraph.segments.map(s => s.text).join(' ')
      });
    }

    console.log(`[INFO] Đã chia thành ${paragraphs.length} đoạn văn (paragraphs).`);
    return paragraphs;
  }

  async callQwenAI(prompt) {
    if (!DASHSCOPE_API_KEY) {
      throw new Error('Thiếu DASHSCOPE_API_KEY trong file .env');
    }

    try {
      const response = await axios.post(
        'https://dashscope-intl.aliyuncs.com/api/v1/services/aigc/text-generation/generation',
        {
          model: 'qwen-plus',
          input: {
            messages: [
              { role: 'system', content: 'Bạn là một trợ lý AI hữu ích, chuyên gia về nội dung sách và kể chuyện.' },
              { role: 'user', content: prompt }
            ]
          },
          parameters: {
            result_format: 'message',
            temperature: 0.7,
            max_tokens: 500
          }
        },
        {
          headers: {
            'Authorization': `Bearer ${DASHSCOPE_API_KEY}`,
            'Content-Type': 'application/json'
          }
        }
      );

      if (response.data.output && response.data.output.choices && response.data.output.choices[0]) {
        return response.data.output.choices[0].message.content;
      }
      throw new Error('Phản hồi API không hợp lệ từ Qwen');
    } catch (error) {
      console.error('[ERROR] Lỗi gọi Qwen API:', error.response?.data || error.message);
      
      if (OPENAI_API_KEY) {
        console.log('[INFO] Thử fallback sang OpenAI...');
        return await this.callOpenAI(prompt);
      }
      throw error;
    }
  }

  async callOpenAI(prompt) {
    try {
      const response = await axios.post(
        'https://api.openai.com/v1/chat/completions',
        {
          model: 'gpt-3.5-turbo',
          messages: [
            { role: 'system', content: 'Bạn là một trợ lý AI hữu ích, chuyên gia về nội dung sách và kể chuyện.' },
            { role: 'user', content: prompt }
          ],
          temperature: 0.7,
          max_tokens: 500
        },
        {
          headers: {
            'Authorization': `Bearer ${OPENAI_API_KEY}`,
            'Content-Type': 'application/json'
          }
        }
      );

      if (response.data.choices && response.data.choices[0]) {
        return response.data.choices[0].message.content;
      }
      throw new Error('Phản hồi API không hợp lệ từ OpenAI');
    } catch (error) {
      console.error('[ERROR] Lỗi gọi OpenAI API:', error.response?.data || error.message);
      throw error;
    }
  }

  async generatePostContent(paragraph, allParagraphs, videoTitle, videoDescription) {
    const currentIndex = allParagraphs.indexOf(paragraph);
    const prevContext = currentIndex > 0 ? allParagraphs[currentIndex - 1].text : '';
    const nextContext = currentIndex < allParagraphs.length - 1 ? allParagraphs[currentIndex + 1].text : '';

    const prompt = `
Bạn là một người yêu sách, muốn chia sẻ một đoạn trích hay từ video YouTube lên mạng xã hội.
Thông tin video:
- Tiêu đề: ${videoTitle}
- Mô tả: ${videoDescription || 'Không có mô tả'}

Đoạn trích chính (Content):
"${paragraph.text}"

Ngữ cảnh trước đó (nếu có): "${prevContext}"
Ngữ cảnh tiếp theo (nếu có): "${nextContext}"

Yêu cầu:
1. Viết một bài đăng ngắn (3-5 câu) giới thiệu đoạn trích này một cách tự nhiên, cảm xúc như một người đọc sách thực thụ.
2. Nêu rõ lý do tại sao đoạn này hay, ý nghĩa gì, hoặc gợi lên cảm xúc gì.
3. Đảm bảo có ngữ cảnh rõ ràng, không viết chung chung.
4. Giọng văn chân thành, gần gũi, không giống robot.
5. Kết thúc bằng một câu hỏi hoặc lời mời thảo luận nhẹ nhàng.

Chỉ trả về nội dung bài đăng, không thêm tiêu đề hay giải thích gì khác.
    `.trim();

    try {
      const content = await this.callQwenAI(prompt);
      return content.trim();
    } catch (error) {
      console.error('[ERROR] Không thể tạo nội dung bài đăng:', error.message);
      return null;
    }
  }

  async extractQuote(paragraph, postContent) {
    const prompt = `
Từ đoạn văn sau và bài đăng kèm theo, hãy trích xuất một câu nói đắt giá nhất (quote) để làm caption ảnh.
Đoạn văn: "${paragraph.text}"
Bài đăng: "${postContent || 'Chưa có'}"

Yêu cầu:
1. Chọn một câu ngắn gọn (dưới 20 từ), súc tích, dễ nhớ và truyền cảm hứng.
2. Nếu không có câu nào phù hợp trong đoạn văn, hãy viết lại một câu tóm tắt ý chính thật hay.
3. Chỉ trả về duy nhất nội dung câu quote, không thêm dấu ngoặc kép hay giải thích.
    `.trim();

    try {
      const quote = await this.callQwenAI(prompt);
      return quote.replace(/^["']|["']$/g, '').trim();
    } catch (error) {
      console.error('[ERROR] Không thể trích xuất quote:', error.message);
      return null;
    }
  }

  async scoreQuote(quote, postContent, paragraphText) {
    const prompt = `
Đánh giá chất lượng của một câu quote (châm ngôn/trích dẫn) dựa trên thang điểm 10.
Quote: "${quote}"
Bài đăng kèm theo: "${postContent || 'Chưa có'}"
Đoạn gốc: "${paragraphText}"

Tiêu chí chấm điểm:
- Sự ngắn gọn, súc tích (tối đa 3 điểm)
- Tính truyền cảm hứng, sâu sắc (tối đa 3 điểm)
- Độ liên quan với ngữ cảnh (tối đa 2 điểm)
- Ngôn ngữ tự nhiên, không sượng (tối đa 2 điểm)

Chỉ trả về một số nguyên từ 0 đến 10, không thêm ký tự nào khác.
    `.trim();

    try {
      const response = await this.callQwenAI(prompt);
      const scoreMatch = response.match(/\d+/);
      if (scoreMatch) {
        return Math.min(10, Math.max(0, parseInt(scoreMatch[0])));
      }
      return 5;
    } catch (error) {
      console.error('[ERROR] Lỗi khi chấm điểm quote:', error.message);
      return 5;
    }
  }

  async saveParagraphs(videoDbId, paragraphs) {
    console.log('[INFO] Đang lưu các đoạn văn (paragraphs) vào database...');
    
    const stmt = await this.connection.prepare(
      `INSERT INTO youtube_paragraphs 
       (ordinal_number, content_raw, content, youtube_video_id, created_at, updated_at) 
       VALUES (?, ?, ?, ?, NOW(6), NOW(6))`
    );

    for (let i = 0; i < paragraphs.length; i++) {
      const p = paragraphs[i];
      await stmt.execute([i + 1, p.content_raw, p.text, videoDbId]);
    }
    
    await stmt.close();
    console.log(`[INFO] Đã lưu ${paragraphs.length} đoạn văn.`);
  }

  async saveQuotes(videoDbId, paragraphs) {
    console.log('[INFO] Đang tạo và lưu quotes/bài đăng...');

    const [videoRows] = await this.connection.execute(
      'SELECT title, description FROM youtube_videos WHERE id = ?',
      [videoDbId]
    );
    
    if (videoRows.length === 0) {
      console.error('[ERROR] Không tìm thấy thông tin video.');
      return;
    }

    const videoTitle = videoRows[0].title || '';
    const videoDescription = videoRows[0].description || '';

    const stmt = await this.connection.prepare(
      `INSERT INTO youtube_quotes 
       (ordinal_number, content, is_visible, youtube_video_id, created_at, updated_at) 
       VALUES (?, ?, ?, ?, NOW(6), NOW(6))`
    );

    let savedCount = 0;

    for (let i = 0; i < paragraphs.length; i++) {
      const paragraph = paragraphs[i];
      console.log(`\n[Xử lý đoạn ${i + 1}/${paragraphs.length}]`);

      const postContent = await this.generatePostContent(paragraph, paragraphs, videoTitle, videoDescription);
      
      if (!postContent) {
        console.log('[SKIP] Bỏ qua do không tạo được bài đăng.');
        continue;
      }

      const quote = await this.extractQuote(paragraph, postContent);
      
      if (!quote) {
        console.log('[SKIP] Bỏ qua do không trích xuất được quote.');
        continue;
      }

      const score = await this.scoreQuote(quote, postContent, paragraph.text);
      const isVisible = score >= 6;

      console.log(`Quote: "${quote}"`);
      console.log(`Score: ${score}/10 -> ${isVisible ? 'HIỂN THỊ' : 'ẨN'}`);

      await stmt.execute([i + 1, quote, isVisible ? 1 : 0, videoDbId]);
      savedCount++;
      
      await new Promise(r => setTimeout(r, 500));
    }

    await stmt.close();
    console.log(`\n[INFO] Đã xử lý và lưu ${savedCount} quotes.`);
  }

  async process(videoId) {
    try {
      await this.connectDB();

      const videoDbId = await this.getVideoId(videoId);
      console.log(`[INFO] Video DB ID: ${videoDbId}`);

      const transcriptItems = await this.fetchTranscript(videoId);

      const mergedSegments = this.mergeSegments(transcriptItems);

      const paragraphs = this.splitIntoParagraphs(mergedSegments);

      if (paragraphs.length === 0) {
        console.warn('[WARN] Không có đoạn văn nào để xử lý.');
        return;
      }

      await this.saveParagraphs(videoDbId, paragraphs);

      await this.saveQuotes(videoDbId, paragraphs);

      console.log('\n[SUCCESS] Hoàn thành xử lý video!');
    } catch (error) {
      console.error('[FATAL] Quá trình xử lý thất bại:', error.message);
      throw error;
    } finally {
      await this.disconnectDB();
    }
  }
}

async function main() {
  const videoId = process.argv[2];

  if (!videoId) {
    console.error('Usage: node youtube_processor.js <VIDEO_ID>');
    console.error('Example: node youtube_processor.js SYwXwiuxO-0');
    process.exit(1);
  }

  const processor = new YouTubeProcessor();
  await processor.process(videoId);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
