require 'json'
require 'net/http'
require 'uri'

namespace :yt_videos_1 do
  YOUTUBE_PARAGRAPH_TEXT_LIMIT_BYTES = 5_000
  YOUTUBE_PARAGRAPH_TARGET_BYTES = 1_800
  YOUTUBE_PARAGRAPH_MIN_BYTES = 800
  YOUTUBE_PARAGRAPH_MAX_SEGMENTS = 8
  YOUTUBE_PARAGRAPH_BREAK_GAP_SECONDS = 1.5

  task fetch_transcript: :environment do
    puts "🚀 Bắt đầu xử lý transcript..."

    # Kiểm tra xem có file ids.txt không
    ids_file = Rails.root.join("lib/tasks/craw_youtube_videos/ids.txt")

    if File.exist?(ids_file)
      puts "📄 Đọc video IDs từ ids.txt..."
      video_ids = File.readlines(ids_file).map(&:strip).reject(&:blank?)

      # Lưu các video chưa có trong DB
      puts "💾 Lưu video vào DB nếu chưa tồn tại..."
      video_ids.each do |vid|
        video = YoutubeVideo.find_by(video_id: vid)
        unless video
          puts "  📥 Lấy metadata cho #{vid}..."
          metadata = fetch_video_metadata(vid)
          if metadata
            YoutubeVideo.create!(metadata.merge(video_id: vid))
          else
            YoutubeVideo.create!(video_id: vid)
          end
        end
      end

      videos = YoutubeVideo.where(video_id: video_ids)
    else
      videos = YoutubeVideo
        .left_joins(:youtube_quotes)
        .where(youtube_quotes: { id: nil })
        .where.not(video_id: nil)
    end

    puts "📊 Tổng video cần xử lý: #{videos.count}"

    videos.find_each(batch_size: 5) do |video|
      begin
        puts "🔍 Đang xử lý #{video.video_id}"

        segments = fetch_by_python(video.video_id)

        save_paragraphs(video, segments)
        save_quotes_with_ai(video, segments)

        puts "✅ Xong #{video.video_id}"

      rescue => e
        puts "❌ Lỗi #{video.video_id}: #{e.message}"
      end
    end

    puts "🎉 Hoàn thành!"
  end

  # ===========================================
  def fetch_by_python(video_id)
    python = Rails.root.join("lib/tasks/craw_youtube_videos/venv/bin/python")
    script = Rails.root.join("lib/tasks/craw_youtube_videos/fetch_transcript.py")

    output = `#{python} #{script} #{video_id} 2>&1`
    data = JSON.parse(output)

    raise data["error"] if data["error"]

    segments = data["segments"]
    raise "Không có transcript" if segments.blank?

    segments
  end

  # ===========================================
  def save_paragraphs(video, segments)
    YoutubeParagraph.where(youtube_video_id: video.id).delete_all

    paragraphs = build_paragraphs(segments)

    paragraphs.each_with_index do |text, i|
      YoutubeParagraph.create!(
        youtube_video_id: video.id,
        ordinal_number: i + 1,
        content_raw: text,
        content: text
      )
    end
  end

  # ===========================================
  def save_quotes_with_ai(video, segments)
    YoutubeQuote.where(youtube_video_id: video.id).delete_all

    groups = segments.each_slice(5).to_a

    groups.each_with_index do |group, i|
      raw_text = group.map { |s| s["text"] }.join(" ")

      # Thử Gemini trước, sau đó fallback sang Grok rồi Groq
      ai_text = generate_with_gemini(raw_text)
      # ai_text = generate_with_grok(raw_text) if ai_text.blank?

      ai_text = normalize_generated_quote(ai_text)
      next if ai_text.blank?

      YoutubeQuote.create!(
        youtube_video_id: video.id,
        ordinal_number: i + 1,
        content: ai_text,
        is_visible: true
      )
    end
  end

  # ===========================================
  def build_paragraphs(
    segments,
    max_bytes: YOUTUBE_PARAGRAPH_TEXT_LIMIT_BYTES,
    target_bytes: YOUTUBE_PARAGRAPH_TARGET_BYTES
  )
    paragraphs = []
    chunk_segments = []

    normalized_segments = segments.filter_map do |segment|
      text = segment["text"].to_s.squish
      next if text.blank?

      {
        "text" => text,
        "start" => segment["start"].to_f,
        "duration" => segment["duration"].to_f
      }
    end

    normalized_segments.each_with_index do |segment, index|
      chunk_segments << segment
      next_segment = normalized_segments[index + 1]

      next unless should_flush_paragraph?(
        chunk_segments,
        next_segment,
        target_bytes: target_bytes,
        max_bytes: max_bytes
      )

      paragraphs.concat(extract_paragraphs_from_chunk(chunk_segments, max_bytes: max_bytes))
      chunk_segments = []
    end

    paragraphs.concat(extract_paragraphs_from_chunk(chunk_segments, max_bytes: max_bytes))
    paragraphs
  end

  def split_long_text(text, max_bytes:)
    return [] if text.blank?

    paragraphs = []
    remaining_text = text.dup

    while remaining_text.bytesize > max_bytes
      split_at = byte_safe_split_index(remaining_text, max_bytes)
      paragraphs << remaining_text.slice!(0, split_at).strip
      remaining_text = remaining_text.lstrip
    end

    paragraphs << remaining_text.strip if remaining_text.present?
    paragraphs
  end

  def byte_safe_split_index(text, max_bytes)
    return text.length if text.bytesize <= max_bytes

    candidate = text.byteslice(0, max_bytes)
    candidate = candidate.to_s

    while candidate.present? && !candidate.valid_encoding?
      candidate = candidate.byteslice(0, candidate.bytesize - 1).to_s
    end

    split_text = candidate.rpartition(" ").first
    split_text = candidate if split_text.blank?

    split_text.length
  end

  def should_flush_paragraph?(chunk_segments, next_segment, target_bytes:, max_bytes:)
    current_text = chunk_segments.map { |segment| segment["text"] }.join(" ").squish
    current_bytes = current_text.bytesize

    return true if current_bytes >= max_bytes
    return false if current_bytes < YOUTUBE_PARAGRAPH_MIN_BYTES

    reached_target_size = current_bytes >= target_bytes
    enough_segments = chunk_segments.length >= YOUTUBE_PARAGRAPH_MAX_SEGMENTS
    natural_break = paragraph_ending?(current_text)
    pause_break = time_gap_break?(chunk_segments.last, next_segment)

    reached_target_size && (natural_break || pause_break || enough_segments)
  end

  def extract_paragraphs_from_chunk(chunk_segments, max_bytes:)
    return [] if chunk_segments.blank?

    text = chunk_segments.map { |segment| segment["text"] }.join(" ").squish
    split_long_text(text, max_bytes: max_bytes)
  end

  def paragraph_ending?(text)
    text.match?(/[.!?…:]["')\]]?\z/)
  end

  def time_gap_break?(current_segment, next_segment)
    return true if next_segment.blank?

    current_end = current_segment["start"].to_f + current_segment["duration"].to_f
    next_start = next_segment["start"].to_f

    (next_start - current_end) >= YOUTUBE_PARAGRAPH_BREAK_GAP_SECONDS
  end

  # ===========================================
  def fetch_video_metadata(video_id)
    api_key = ENV["YOUTUBE_API_KEY"]
    return nil if api_key.blank?

    uri = URI("https://www.googleapis.com/youtube/v3/videos")
    uri.query = URI.encode_www_form({
      id: video_id,
      key: api_key,
      part: "snippet,contentDetails"
    })

    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true

    request = Net::HTTP::Get.new(uri)
    response = http.request(request)
    data = JSON.parse(response.body)

    return nil if data["items"].blank?

    item = data["items"].first
    snippet = item["snippet"]

    {
      kind: item["kind"],
      etag: item["etag"],
      title: snippet["title"],
      description: snippet["description"],
      thumbnail: snippet.dig("thumbnails", "default", "url"),
      channel_id: snippet["channelId"],
      channel_title: snippet["channelTitle"],
      publish_time: snippet["publishedAt"],
      published_at: snippet["publishedAt"],
      snippet: snippet.to_json
    }
  rescue => e
    puts "⚠️ YouTube API lỗi: #{e.message}"
    nil
  end

  def build_quote_prompt(text)
    <<~PROMPT
      Từ đoạn transcript sau, hãy viết một caption ngắn, sâu và dễ chia sẻ để đăng bài.

      Transcript:
      "#{text}"

      Yêu cầu:
      - Viết tự nhiên, có cảm xúc, giống một người vừa đọc xong muốn chia sẻ lại.
      - Ưu tiên chọn một câu trích nguyên văn thật sự có ý nghĩa, đọc riêng vẫn hiểu, có thể khiến người khác dừng lại suy nghĩ.
      - Câu trích phải là một câu hoàn chỉnh hoặc gần hoàn chỉnh, đủ ý, không lấy mẩu quá ngắn, không cắt nửa câu, không lấy cụm từ rời rạc.
      - Không chọn những câu trích chung chung, nhạt, hoặc cần quá nhiều ngữ cảnh mới hiểu.
      - Nếu transcript không có câu nào đủ hay và đủ trọn ý để trích nguyên văn, thì đừng cố chèn dấu ngoặc kép; hãy viết caption chỉ với một ý cảm nhận ngắn nhưng rõ nghĩa.
      - Nếu có câu trích phù hợp, caption nên có 2 phần trong cùng một đoạn ngắn:
        1. Một câu cảm nhận ngắn.
        2. Một câu trích nguyên văn từ transcript đặt trong dấu ngoặc kép.
      - Nếu trong transcript có tên sách, chỉ nhắc tên sách khi thực sự liên quan và tự nhiên.
      - Không bịa tên sách, không bịa câu trích, không thêm ý không có trong transcript.
      - Tối đa 2 câu.
      - Ưu tiên câu trích dài khoảng 12 đến 35 từ nếu có thể.

      Chỉ trả về caption cuối cùng, không giải thích.

      Ví dụ đầu ra:
      Đọc tới đây mình thấy điều người ta cần đôi khi chỉ là được thấu hiểu thật lòng: "Ai cũng muốn được trân trọng và lắng nghe."
    PROMPT
  end

  def normalize_generated_quote(text)
    result = text.to_s.squish
    return nil if result.blank?

    quoted_texts = result.scan(/"([^"]+)"/).flatten
    return result if quoted_texts.blank?

    quote = quoted_texts.max_by { |item| item.length }.to_s.squish
    return nil if quote.blank?

    word_count = quote.scan(/\S+/).size
    return nil if word_count < 8

    result
  end

  # ===========================================
  def generate_with_grok(text)
    api_key = "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"
    return nil if api_key.blank?

    uri = URI("https://api.x.ai/v1/chat/completions")

    prompt = build_quote_prompt(text)

    body = {
      model: "grok-3",
      messages: [
        {
          role: "user",
          content: prompt
        }
      ],
      temperature: 0.7,
      max_tokens: 200
    }

    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true

    request = Net::HTTP::Post.new(uri)
    request['Authorization'] = "Bearer #{api_key}"
    request['Content-Type'] = 'application/json'
    request.body = body.to_json

    response = http.request(request)
    json = JSON.parse(response.body)

    unless json["choices"]
      puts "⚠️ Grok raw response: #{json}"
      return nil
    end

    text = json.dig("choices", 0, "message", "content")
    text.to_s.strip
  rescue => e
    puts "⚠️ Grok lỗi: #{e.message}"
    nil
  end

  # ===========================================
  def generate_with_gemini(text)
    api_key = [
        "AIzaSyBIsSyFNutubWm9_ANjXBM4l9eN1_wIhio"
      ].flatten.sample
    raise "Thiếu GEMINI_API_KEY" if api_key.blank?

    uri = URI("https://generativelanguage.googleapis.com/v1/models/gemini-2.0-flash:generateContent?key=#{api_key}")

    prompt = build_quote_prompt(text)

    body = {
      contents: [
        {
          parts: [
            { text: prompt }
          ]
        }
      ]
    }

    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true

    request = Net::HTTP::Post.new(uri)
    request['Content-Type'] = 'application/json'
    request.body = body.to_json

    response = http.request(request)
    json = JSON.parse(response.body)

    # 🔥 DEBUG QUAN TRỌNG
    unless json["candidates"]
      puts "⚠️ Gemini raw response: #{json}"
      return nil
    end

    text = json.dig("candidates", 0, "content", "parts", 0, "text")

    text.to_s.strip
  rescue => e
    puts "⚠️ Gemini lỗi: #{e.message}"
    nil
  end
end

