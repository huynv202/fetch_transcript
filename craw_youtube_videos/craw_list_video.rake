require 'json'
require 'net/http'
require 'uri'
require 'cgi'
require 'nokogiri'
require 'fuzzy_match'
require 'amatch'
require 'set'


namespace :yt_videos do
  task :init => :environment do
    # delete all data
    # YoutubeVideo.destroy_all
    api_key = "AIzaSyBdyeJ0VdOUAuF3fq39T8copQNoKhm92rw"
    channel_ids = ["UCgKVnpqft_rMv2WmNTQyVdg"]
    channel_ids.each do |channel_id|
      url_base = "https://www.googleapis.com/youtube/v3/search?key=#{api_key}&channelId=#{channel_id}&part=snippet&type=video&order=date&maxResults=40"
      page_token = ""

      # get data from url
      # is_exist = false
      while true
        url = url_base
        if page_token != ""
          url = url_base + "&pageToken=" + page_token
        end
        # puts "*" * 50
        # puts url

        uri = URI.parse(url)
        response = Net::HTTP.get_response(uri)
        data = JSON.parse(response.body)

        # get video data
        data["items"].each do |item|
          kind = item["id"]["kind"]
          etag = item["etag"]
          video_id = item["id"]["videoId"]
          channel_id = item["snippet"]["channelId"]
          channel_title = item["snippet"]["channelTitle"]
          title = item["snippet"]["title"]
          description = item["snippet"]["description"]
          thumbnail = item["snippet"]["thumbnails"]["default"]["url"]
          publish_time = item["snippet"]["publishTime"]
          published_at = item["snippet"]["publishedAt"]
          snippet = item["snippet"]

          if !title.downcase.include?("[Sách Nói]".downcase)
            # puts "Title contains [Sách Nói]"
            # puts title
            # puts video_id
            next
          end

          #check if video_id already exists
          if YoutubeVideo.exists?(video_id: video_id)
            # || !title.downcase.include?("[Sách Nói]".downcase)
            # is_exist = true
            # puts "Video ID already exists"
            # puts title
            # puts video_id
            next
          end
          # save to database
          YoutubeVideo.create(
            kind: kind,
            etag: etag,
            video_id: video_id,
            channel_id: channel_id,
            channel_title: channel_title,
            title: title,
            description: description,
            thumbnail: thumbnail,
            publish_time: publish_time,
            published_at: published_at,
            snippet: snippet
          )

          # puts "Đã thêm video: #{title}"
          # puts "id: #{video_id}"

        end

        # get next page token
        page_token = data["nextPageToken"]
        # puts page_token
        # puts "*" * 50
        break if page_token == nil || page_token == ""
      end
    end
  end

  task :get_raw_paragraph => :environment do

    # YoutubeParagraph.destroy_all

    # # puts "xoa xong"

    # add_paragraph_for_youtube_video("OVr0lYmgB84", 1)

    videos = YoutubeVideo.all
    count = 0
    videos.each do |video|
      # # puts video.title
      video_id = video.video_id
      youtube_video_id = video.id
      if YoutubeParagraph.where(youtube_video_id: youtube_video_id).length > 0
        # puts "Video ID already exists: #{video.title}"
        next
      end
      # puts "Tên video: #{video.title}"
      # puts "ID video: #{video_id}"
      add_paragraph_for_youtube_video(video_id, youtube_video_id)
      count += 1
      # puts "*" * 25 + count.to_s + "*" * 25
    end
  end

  task :gen_content => :environment do
    count = 0
    YoutubeParagraph.all.each do |paragraph|
      prompt = paragraph.content_raw
      content = paragraph.content
      puts "---------id-#{paragraph.id}-------------------"
      if content == nil || content == ""
        puts prompt
        puts "*" * 50
        # prompt += ". Bạn hãy thêm dấu chấm, dấu phẩy để ngắt câu cho đoạn văn trên. Không bổ sung thêm từ"
        prompt = <<~PROMPT
          Đây là đoạn transcript từ video, đang rất thô và thiếu dấu:

          "#{prompt}"

          Hãy viết lại thành một đoạn văn hoàn chỉnh:
          - Giữ nguyên ý
          - Thêm dấu câu tự nhiên
          - Văn phong giống người viết bài chia sẻ
          - Dễ đọc, mượt, có cảm xúc nhẹ
          - Không thêm thông tin ngoài nội dung gốc
          PROMPT
        result = call_api_gemini(prompt)
        puts "*" * 25 + "content" + "*" * 25
        puts result
        puts "*" * 50
        # cập nhật youtube_paragraphs
        paragraph.update(content: result)
      else
        puts "Content is not nil"
      end
      content = paragraph.content
      db_quotes = YoutubeQuote.where(ordinal_number: paragraph.ordinal_number, youtube_video_id: paragraph.youtube_video_id)
      if db_quotes.length == 0
        prompt = "Tôi đang phát triển 1 mạng xã hội mà mọi người đăng sách lên và có viết về quyển sách đó bằng một bài viết. Nên từ đoạn văn dưới đây bạn chọn ra các câu văn hoặc đoạn văn hay nhất từ đoạn văn này mà giữ nguyên đoạn văn gốc, không thay đổi gì cả mà nó có thể làm caption của quyền sách rồi trả lại cho tôi dữ liệu theo dạng (kết quả trả ra phải mang thêm phong cách nói chuyện hoặc đăng bài của con người để gần gũi hơn với người đọc): | đoạn văn 1 | | đoạn văn 2 | ... |đoạn văn n|. " + content
        puts "prompt: #{prompt}"
        result = call_api_gemini(prompt)
        puts "*" * 50
        puts result
        quotes = result.scan(/\|([^\|]+)\|/)
        # Hiển thị các đoạn đã tách
        quotes.each_with_index do |para, index|
          if para[0].length > 10
            puts para[0]
            YoutubeQuote.create(
              ordinal_number: paragraph.ordinal_number,
              content: para[0],
              youtube_video_id: paragraph.youtube_video_id,
              is_visible: false
              )
            puts "*" * 50
          end
        end
      else
        puts "Quotes is not nil. O_Number: #{paragraph.ordinal_number}, V_Id: #{paragraph.youtube_video_id}"
      end
    end
    # youtube_video_id = 2599
    # paragraphs = YoutubeParagraph.where(youtube_video_id: youtube_video_id)
    # paragraphs.each do |paragraph|
    #   prompt = paragraph.content_raw
    #   # puts "---------id-#{paragraph.id}-------------------"
    #   # puts prompt
    #   # puts "*" * 50
    #   prompt += ". Bạn hãy thêm dấu chấm, dấu phẩy để ngắt câu cho đoạn văn trên. Không bổ sung thêm từ"
    #   # prompt += ". Bạn hãy thêm dấu chấm để ngắt câu cho đoạn văn trên và tìm ra đoạn văn hay nhất trong đoạn văn trên để tôi có thể sử dụng nó để đăng tải làm miêu tả cho chủ đề #{youtube_video.title} và trả lại cho tôi (lưu ý tôi chỉ cần đoạn văn đó đó, không cần những thứ khác nên bạn không cần diễn đạt!)"
    #   result = call_api_gemini(prompt)
    #   # puts "*" * 50
    #   # puts result
    #   # cập nhật youtube_paragraphs
    #   paragraph.update(content: result)
    # end
  end

  # task :gen_quote => :environment do
  #   # YoutubeParagraph.all.each do |paragraph|
  #   #   content = paragraph.content
  #   #   youtube_video = YoutubeVideo.find(paragraph.youtube_video_id)
  #   #   if !content.nil? && content != ""
  #   #     # puts "---------id-#{paragraph.id}-------------------"
  #   #     prompt = content + ". Bạn hãy tìm ra đoạn văn hay nhất trong đoạn văn trên để tôi có thể sử dụng nó để làm miêu tả cho chủ đề #{youtube_video.title} và trả lại cho tôi (lưu ý tôi chỉ cần đoạn văn đó đó, không cần những thứ khác nên bạn không cần diễn đạt!)"
  #   #     result = call_api_gemini(prompt)
  #   #     YoutubeQuote.create(
  #   #       ordinal_number: paragraph.ordinal_number,
  #   #       content: result,
  #   #       youtube_video_id: paragraph.youtube_video_id,
  #   #       is_visible: false
  #   #       )
  #   #     # puts "*" * 50
  #   #   end
  #   # end

  #   YoutubeParagraph.all.each do |paragraph|
  #     content = paragraph.content
  #     # youtube_video = YoutubeVideo.find(paragraph.youtube_video_id)
  #     if !content.nil? && content != ""
  #       # puts "---------id-#{paragraph.id}-------------------"
  #       prompt = "Tôi đang phát triển 1 mạng xã hội mà mọi người đăng sách lên và có viết về quyển sách đó bằng một bài viết. Nên từ đoạn văn dưới đây bạn chọn ra các câu văn hoặc đoạn văn hay nhất từ đoạn văn này mà giữ nguyên đoạn văn gốc, không thay đổi gì cả mà nó có thể làm caption của quyền sách rồi trả lại cho tôi dữ liệu theo dạng: | đoạn văn 1 | | đoạn văn 2 | ... |đoạn văn n|. " + content
  #       result = call_api_gemini(prompt)
  #       # puts "*" * 50
  #       # puts result
  #       quotes = result.scan(/\|([^\|]+)\|/)
  #       # Hiển thị các đoạn đã tách
  #       quotes.each_with_index do |para, index|
  #         if para[0].length > 10
  #           # puts para[0]
  #           YoutubeQuote.create(
  #             ordinal_number: paragraph.ordinal_number,
  #             content: para[0],
  #             youtube_video_id: paragraph.youtube_video_id,
  #             is_visible: false
  #             )
  #           # puts "*" * 50
  #         end
  #       end
  #     end
  #   end
  # end

  # đọc dữ liệu từ file ids.txt đã lấy bằng tay và lưu vào database
  task :craw_youtube_videos => :environment do
    file_path = File.join(Dir.pwd, "lib/tasks/craw_youtube_videos/ids.txt")
    video_ids = []
    File.open(file_path, "r") do |f|
      f.each_line do |line|
        if line.strip != ""
          video_ids << line.strip
        end
      end
    end
    # puts video_ids.length
    api_keys = [
      "AIzaSyCEHnv7rv6GBbQa1wKkOPnwsNIz3XMzuUs",
    ]
    channel_id_fonos = "UCgKVnpqft_rMv2WmNTQyVdg"
    count = 0;
    video_ids.each do |video_id|
      #lấy api key random
      # puts "*" * 20 + video_id + "*" * 20
      count += 1
      # puts count
      if YoutubeVideo.exists?(video_id: video_id)
        # puts "Video ID already exists"
        # puts video_id
        next
      end
      random_index = rand(api_keys.length)
      api_key = api_keys[random_index]
      url_base = "https://www.googleapis.com/youtube/v3/videos?id=#{video_id}&part=snippet,contentDetails,statistics&key=#{api_key}"
      url = url_base
      uri = URI.parse(url)
      response = Net::HTTP.get_response(uri)
      data = JSON.parse(response.body)
      if data["items"].length > 0
        item = data["items"][0]
        kind = item["kind"]
        etag = item["etag"]
        video_id = item["id"]
        channel_id = item["snippet"]["channelId"]
        channel_title = item["snippet"]["channelTitle"]
        title = item["snippet"]["title"]
        description = item["snippet"]["description"]
        thumbnail = item["snippet"]["thumbnails"]["default"]["url"]
        publish_time = item["snippet"]["publishTime"]
        published_at = item["snippet"]["publishedAt"]
        snippet = item["snippet"]
        if channel_id == channel_id_fonos
          # puts "create video"
          YoutubeVideo.create(
            kind: kind,
            etag: etag,
            video_id: video_id,
            channel_id: channel_id,
            channel_title: channel_title,
            title: title,
            description: description,
            thumbnail: thumbnail,
            publish_time: publish_time,
            published_at: published_at,
            snippet: snippet
          )
        else
          # puts "Not Fonos channel"
          # puts title
          # puts video_id
        end
      end
    end
  end

  #xóa các video không phải sách
  # task :delete_videos => :environment do
  #   file_path = File.join(Dir.pwd, "lib/tasks/craw_youtube_videos/id_remove.txt")
  #   video_ids = []
  #   File.open(file_path, "r") do |f|
  #     f.each_line do |line|
  #       if line.strip != ""
  #         video_ids << line.strip
  #       end
  #     end
  #   end
  #   # puts video_ids.length
  #   api_keys = [
  #     "AIzaSyCEHnv7rv6GBbQa1wKkOPnwsNIz3XMzuUs",
  #   ]
  #   video_ids.each do |video_id|
  #     # puts "*" * 20 + video_id + "*" * 20
  #     YoutubeVideo.where(id: video_id).destroy_all
  #   end
  #   YoutubeVideo.where(id: 1232).destroy_all
  # end

  task :test => :environment do
    all_youtube_videos = YoutubeVideo.all
    all_fahasa_books = FahasaBook.all
    youtube_titles = all_youtube_videos.map { |video| {
      id: video.id,
      title: video.title
    } }
    fahasa_name = all_fahasa_books.map { |book| {
      id: book.id,
      name: book.name
    } }
    fuzzy_match = FuzzyMatch.new(fahasa_name, read: :name)
    result = fuzzy_match.find("Ươm mầm")
    # puts result

    # id_processed = []
    # File.open("lib/tasks/craw_youtube_videos/id_processed.txt", "r") do |f|
    #   f.each_line do |line|
    #     if line.strip != ""
    #       id_processed << line.strip
    #     end
    #   end
    # end

    # File.open("lib/tasks/craw_youtube_videos/youtube_fahasa_result.txt", "a") do |f|
    #   youtube_titles.each do |yt_title|

    #     if id_processed.include?(yt_title[:id].to_s)
    #       # puts "Video ID already processed"
    #       # puts yt_title[:id]
    #       next
    #     end

    #     result = fuzzy_match.find(yt_title[:title])
    #     # puts "-" * 100
    #     # puts "Video ID: #{yt_title[:id]} - Title: #{yt_title[:title]}"
    #     f.# puts "-" * 100
    #     f.# puts yt_title
    #     # puts "Best match: #{result[:name]} (Book ID: #{result[:id]})"
    #     f.# puts result
    #     File.open("lib/tasks/craw_youtube_videos/id_processed.txt", "a") do |f|
    #       f.# puts yt_title[:id]
    #     end
    #   end
    # end
  end

  task :fuzzy_match => :environment do
    youtube_videos = YoutubeVideo.all

    id_processed = []
    File.open("lib/tasks/craw_youtube_videos/id_processed.txt", "r") do |f|
      f.each_line do |line|
        if line.strip != ""
          id_processed << line.strip
        end
      end
    end

    search_videos = []

    # File.open("lib/tasks/craw_youtube_videos/youtube_fahasa_result.txt", "a") do |f|
      youtube_videos.each do |video|
        if id_processed.include?(video.id.to_s)
          puts "Video ID already processed"
          puts video.id
          next
        end
        title_edited = video.title.gsub(/\[.*?\]|\(.*?\)|\|.*$/, "").gsub(/ - Chương \d+/, "").gsub(/ - Tập \d+/, "").gsub(/ - Phần \d+/, "").gsub(/ - Full/, "").strip
        search_video = {
          youtube_video_id: video.id,
          youtube_video_title: video.title,
          youtube_video_title_edited: title_edited
        }
        puts "Processing video id: #{video.id}"
        puts "Processing title: #{video.title}"
        puts "Processing title edited: #{title_edited}"
        # f.puts "YouTube Video ID: #{video.id}"
        # f.puts "YouTube Video Title: #{video.title}"
        # f.puts "YouTube Video Title Edited: #{title_edited}"
        fahasa_books = find_fhs(video)
        if fahasa_books.any?
          fuzzy_match = FuzzyMatch.new(fahasa_books, read: :name)
          result = fuzzy_match.find(video.title)
          if result
            puts "-----> Fahasa id: #{result.id}"
            puts "-----> Fahasa name: #{result.name}"
            # f.puts "----->>>>>Fahasa Book ID: #{result.id}"
            # f.puts "----->>>>>Fahasa Book Name: #{result.name}"
            levenshtein = Amatch::Levenshtein.new(title_edited)
            distance = levenshtein.match(result.name)
            similarity = 1 - distance.to_f / [title_edited.length, result.name.length].max
            search_video[:fahasa_book_id] = result.id
            search_video[:fahasa_book_name] = result.name
            search_video[:distance] = distance
            search_video[:similarity] = similarity
            search_video[:is_match] = true
            puts "----->>>>> Distance: #{distance}"
            puts "----->>>>> Similarity: #{similarity}"
            # f.puts "----->>>>> Distance: #{distance}"
            # f.puts "----->>>>> Similarity: #{similarity}"

          else
            puts "Không tìm thấy khớp cho video: '#{video.title}'"
            search_video[:is_match] = false
            # f.puts ":(((((((((((( Không tìm thấy khớp cho video"
          end
          File.open("lib/tasks/craw_youtube_videos/id_processed.txt", "a") do |f|
            f.puts video.id
          end
        else
          puts "Không tìm thấy khớp cho video: '#{video.title}'"
          search_video[:is_match] = false
          # f.puts ":(((((((((((( Không tìm thấy khớp cho video"
        end
        # f.puts "-" * 100
        search_videos << search_video
        puts "-" * 100
      end
    # end
    File.open("lib/tasks/craw_youtube_videos/youtube_fahasa_result.json", "w") do |f|
      f.puts JSON.pretty_generate(search_videos)
    end
  end

  task :fuzzy_match_2 => :environment do
    youtube_videos = YoutubeVideo.all

    id_processed = []
    File.open("lib/tasks/craw_youtube_videos/id_processed.txt", "r") do |f|
      f.each_line do |line|
        if line.strip != ""
          id_processed << line.strip
        end
      end
    end
    search_videos = []
    File.open("lib/tasks/craw_youtube_videos/youtube_fahasa_result.json", "r") do |f|
      search_videos = JSON.parse(f.read)
    end

    File.open("lib/tasks/craw_youtube_videos/youtube_fahasa_result.txt", "a") do |f|
      youtube_videos.each do |video|
        # puts video.inspect
        if video.fahasa_book_id != nil
          puts "Video ID already has Fahasa ID"
          puts video.id
          next
        end
        if id_processed.include?(video.id.to_s)
          puts "Video ID already processed"
          puts video.id
          next
        end
        title_edited = video.title.gsub(/\[.*?\]|\(.*?\)|\|.*$/, "").gsub(/ - Chương \d+/, "").gsub(/ - Tập \d+/, "").gsub(/ - Phần \d+/, "").gsub(/ - Full/, "").strip
        search_video = {
          youtube_video_id: video.id,
          youtube_video_title: video.title,
          youtube_video_title_edited: title_edited
        }
        puts "Processing video id: #{video.id}"
        puts "Processing title: #{video.title}"
        puts "Processing title edited: #{title_edited}"
        f.puts "YouTube Video ID: #{video.id}"
        f.puts "YouTube Video Title: #{video.title}"
        # f.puts "YouTube Video Title Edited: #{title_edited}"
        fahasa_books = find_fhs(video)
        if fahasa_books.any?
          fahasa_books_combined = fahasa_books.map { |book| {
            id: book.id,
            name: book.name,
            name_author: book.name + " | " + book.author.to_s
          }}
          puts "Fahasa books combined: #{fahasa_books_combined}"
          fuzzy_match = FuzzyMatch.new(fahasa_books_combined, read: :name_author)
          title_gsub_1 = video.title.gsub(/\[.*?\]/, '')
          puts "&*($@@$@!#@!$)))------> Title gsub []: #{title_gsub_1}"
          result = fuzzy_match.find(title_gsub_1)
          if result
            puts "-----> Fahasa id: #{result[:id]}"
            puts "-----> Fahasa name: #{result[:name]}"
            # f.puts "----->>>>>Fahasa Book ID: #{result[:id]}"
            f.puts "----->>>>>Fahasa Book Name: #{result[:name]}"
            # levenshtein = Amatch::Levenshtein.new(title_edited)
            # distance = levenshtein.match(result.name)
            # similarity = 1 - distance.to_f / [title_edited.length, result.name.length].max
            search_video[:fahasa_book_id] = result[:id]
            search_video[:fahasa_book_name] = result[:name]
            # search_video[:distance] = distance
            # search_video[:similarity] = similarity
            search_video[:is_match] = true
            # puts "----->>>>> Distance: #{distance}"
            # puts "----->>>>> Similarity: #{similarity}"
            # f.puts "----->>>>> Distance: #{distance}"
            # f.puts "----->>>>> Similarity: #{similarity}"

          else
            puts "Không tìm thấy khớp cho video: '#{video.title}'"
            search_video[:is_match] = false
            f.puts ":(((((((((((( Không tìm thấy khớp cho video"
          end
          File.open("lib/tasks/craw_youtube_videos/id_processed.txt", "a") do |f|
            f.puts video.id
          end
        else
          puts "Không tìm thấy khớp cho video: '#{video.title}'"
          search_video[:is_match] = false
          f.puts ":(((((((((((( Không tìm thấy khớp cho video"
        end
        f.puts "-" * 100
        search_videos << search_video
        File.open("lib/tasks/craw_youtube_videos/youtube_fahasa_result.json", "w") do |f|
          f.puts JSON.pretty_generate(search_videos)
        end
        puts "-" * 100
      end
    end
  end

  task :add_fahasa_id => :environment do
    search_videos = []
    File.open("lib/tasks/craw_youtube_videos/youtube_fahasa_result.json", "r") do |f|
      search_videos = JSON.parse(f.read)
    end
    filtered_search_video_ids = []
    File.open("lib/tasks/craw_youtube_videos/youtube_fahasa_result.txt", "r") do |f|
      f.each_line do |line|
        if line.strip != ""
          if line.include?("YouTube Video ID:")
            video_id = line.gsub("YouTube Video ID: ", "").strip
            filtered_search_video_ids << video_id
          end
        end
      end
    end
    puts "Filtered search video IDs: #{filtered_search_video_ids}"
    search_videos.each do |search_video|
      if search_video["is_match"] && filtered_search_video_ids.include?(search_video["youtube_video_id"].to_s)
        puts "YouTube video ID: #{search_video["youtube_video_id"]}"
        puts "Vides title: #{search_video["youtube_video_title"]}"
        puts "Fahasa book name: #{search_video["fahasa_book_name"]}"
        puts "Fahasa book ID: #{search_video["fahasa_book_id"]}"
        puts "-" * 100
        youtube_video = YoutubeVideo.find(search_video["youtube_video_id"])
        fahasa_book_id = search_video["fahasa_book_id"]
        youtube_video.update(fahasa_book_id: fahasa_book_id)
      end
    end
  end


  task :replace => :environment do
    # sử dụng where like để tìm kiếm sách
    FahasaBook.all.each do |book|
      book.update(url: book.url.gsub("?fhs_campaign=CATEGORY", ""))
      puts book.url
    end

  end

  task :get_raw_sub_by_id => :environment do
    video_id = "5BM-_qvGXjI"
    url = "https://www.youtube.com/watch?v=#{video_id}"

    # Lấy nội dung của trang YouTube
    uri = URI(url)
    response = Net::HTTP.get(uri)

    text = ""

    # Kiểm tra sự tồn tại của auto-generated caption
    has_auto_generated = response.match("kind=asr")

    # Lấy URL của timedtext API
    link_rg = response.match(/https\:\/\/www\.youtube\.com\/api\/timedtext\?.*?"/)

    if link_rg
      timedtext_url = link_rg[0]
      timedtext_url = timedtext_url.gsub('"', '').gsub('\\u0026', '&')
      timedtext_uri = URI(timedtext_url)
      timedtext_response = Net::HTTP.get(timedtext_uri)
      xml_doc = Nokogiri::XML(timedtext_response)
      # quotes = [];
      # full_text = ""
      xml_doc.xpath('//text').each do |text_node|
        start_time = text_node['start']  # Thuộc tính start
        duration = text_node['dur']      # Thuộc tính dur
        text_content = text_node.text.strip  # Nội dung văn bản trong thẻ
        if text_content != "[âm nhạc]"
          text += "--start :: #{start_time} --duration :: #{duration} --content :: #{text_content}\n"
        end
      end
      File.open("lib/tasks/craw_youtube_videos/xml_sub.txt", "w") do |f|
        f.write(text)
      end
    else
      # puts "Không tìm thấy timedtext"
    end
  end

  task :check_para_to_quote => :environment do
    count = 0
    YoutubeParagraph.all.each do |paragraph|
      if paragraph.youtube_video_id != 1522
        next
      end
      prompt = paragraph.content_raw
      content = paragraph.content
      # puts "---------id-#{paragraph.id}-------------------"
      # if content == nil || content == ""
        # puts prompt
        # puts "*" * 50
        # # prompt = "Giúp tôi thêm dấu câu (, . ? ! :) vào đoạn văn dưới đây mà không thay đổi bất kỳ từ ngữ nào, kể cả sửa lỗi chính tả hay là thay đổi cấu trúc câu kể cả dấu thanh trong tiếng việt. " + paragraph.content_raw
        # prompt = "Thêm dấu câu vào đoạn văn dưới đây cho chính xác, giữ nguyên từ ngữ. Chỉ trả về kết quả. " + paragraph.content_raw

        # result = call_api_gemini(prompt)
        # puts "*" * 25 + "content" + "*" * 25
        # puts result
      #   puts "*" * 50
      #   # cập nhật youtube_paragraphs
      #   paragraph.update(content: result)
      # else
      #   puts "Content is not nil"
      # end
      content = paragraph.content
      db_quotes = YoutubeQuote.where(ordinal_number: paragraph.ordinal_number, youtube_video_id: paragraph.youtube_video_id)
      # if db_quotes.length == 0
        prompt = "Tôi đang phát triển 1 mạng xã hội mà mọi người đăng sách lên và có viết về quyển sách đó bằng một bài viết. Nên từ đoạn văn dưới đây bạn chọn ra các câu văn hoặc đoạn văn hay nhất từ đoạn văn này mà giữ nguyên đoạn văn gốc, không thay đổi gì cả mà nó có thể làm caption của quyền sách rồi trả lại cho tôi dữ liệu theo dạng: | đoạn văn 1 | | đoạn văn 2 | ... |đoạn văn n|. " + content
        result = call_api_gemini(prompt)
        puts "*" * 50
        # puts result
        quotes = result.split("|")
        # Hiển thị các đoạn đã tách
        # puts quotes.length
        quotes.each_with_index do |para, index|

          if para != nil && para.strip.length > 0
            puts para
            # YoutubeQuote.create(
            #   ordinal_number: paragraph.ordinal_number,
            #   content: para[0],
            #   youtube_video_id: paragraph.youtube_video_id,
            #   is_visible: false
            #   )
            puts "*" * 50
          end
        end
        sleep(5)
      # else
      #   puts "Quotes is not nil. O_Number: #{paragraph.ordinal_number}, V_Id: #{paragraph.youtube_video_id}"
      # end
    end
  end

  def find_fhs(video)
    result = Set.new
    title = video.title.strip  # Lấy title của video và loại bỏ khoảng trắng thừa
    title = title.gsub(/\[.*?\]|\(.*?\)|\|.*$/, "").gsub(/ - Chương \d+/, "").gsub(/ - Tập \d+/, "").gsub(/ - Phần \d+/, "").gsub(/ - Full/, "").strip  # Loại bỏ các thông tin không cần thiết trong title
    original_title = title  # Lưu lại title gốc để kiểm tra sau
    words = title.split(' ')  # Chia title thành các từ
    # puts "-"*100
    # # puts "Ori Title: #{video.title}"
    # puts "Video ID: #{video.id}"
    # puts "Original Title: #{video.title}"
    # puts "Edited Title: #{title}"
    is_match = false

    # Tìm kiếm sách có tên chứa title hoàn chỉnh
    books = FahasaBook.where("name LIKE ? COLLATE utf8mb4_unicode_ci", "%#{title}%")  # Sử dụng LIKE cho tìm kiếm không phân biệt chữ hoa chữ thường

    if books.any?
      # Nếu tìm thấy sách có tên chứa toàn bộ title
      result.merge(books)
      # books.each do |book|
      #   # puts "Tìm thấy khớp cho video: '#{video.title}' với sách: '#{book.name}'"
      #   is_match = true
      # end
    end

    while !words.empty?
      # Nối lại title sau khi loại bỏ một số từ
      partial_title = words.join(' ')

      # Tìm kiếm sách có tên chứa phần title hiện tại
      books = FahasaBook.where("name LIKE ?", "%#{partial_title}%")

      if books.any?
        # Nếu tìm thấy sách có tên chứa partial_title
        result.merge(books)
        # books.each do |book|
        #   # puts "Tìm thấy khớp cho video: '#{original_title}' với sách: '#{book.name}' (sau khi loại bỏ từ)"
        #   is_match = true
        # end
        break
      end
      # puts "run--------------- 1"
      words.pop  # Loại bỏ từ cuối cùng trong title
    end

    words = title.split(' ')

    # Nếu không tìm thấy, bắt đầu loại bỏ từ đầu tiên trong title và tìm lại
    while !words.empty?
      # Loại bỏ từ đầu tiên trong title
      words.shift  # Shift bỏ từ đầu tiên trong title

      # Nối lại title sau khi loại bỏ từ đầu tiên
      partial_title = words.join(' ')

      # Tìm kiếm sách có tên chứa partial_title
      books = FahasaBook.where("name LIKE ?", "%#{partial_title}%")

      if books.any?
        # puts "2 #{books.length}"
        result.merge(books)
        # result = books
        # Nếu tìm thấy sách có tên chứa partial_title
        # books.each do |book|
        #   if result.include?(book)
        #     next
        #   end
        #   result << book
        #   # puts "Tìm thấy khớp cho video: '#{original_title}' với sách: '#{book.name}' (sau khi loại bỏ từ đầu tiên)"
        #   is_match = true
        #   puts "hỏng"
        # end
        break
      end
      # puts "run--------------- 2"

    end
    # puts "123213asdasda"

    # Ghi lại title không tìm thấy khớp
    if !is_match
      # puts "Không tìm thấy khớp cho video: '#{original_title}'"
    end

    # puts "-"*100
    return result
  end




  def split_text(text, chunk_size, overlap_size)
    chunks = []
    length = text.length
    index = 0

    while index < length
      # Xác định đoạn văn bản cần cắt
      start_index = [index - overlap_size, 0].max
      end_index = [index + chunk_size + overlap_size, length].min
      chunk = text[start_index...end_index]

      chunks << chunk

      # Di chuyển chỉ số index theo chiều dài của chunk trừ đi phần overlap
      index += chunk_size
    end

    chunks
  end

  def add_paragraph_for_youtube_video(video_id, youtube_video_id)
    url = "https://www.youtube.com/watch?v=#{video_id}"

    # Lấy nội dung của trang YouTube
    uri = URI(url)
    response = Net::HTTP.get(uri)

    text = ""
    prompt = ". Với đoạn trích trên, bạn hãy ngắt câu bằng dấu chấm và trả lại cho tôi đoạn trích đó(lưu ý tôi chỉ cần kết quả, không cần những thứ khác nên bạn không cần diễn đạt!)"
    # prompt = ". Với đoạn trích trên, bạn hãy ngắt câu bằng dấu chấm và trả lại cho tôi đoạn trích đó được chia thành các đoạn văn (lưu ý, đoạn văn nằm trong [] chứ không phải từng câu nằm trong [] và tôi chỉ cần kết quả, không cần những thứ khác nên bạn không cần diễn đạt!)"
    # prompt = ". Bạn tìm những câu có ý nghĩa và là luận điểm giúp tôi (lưu ý tôi chỉ cần danh sách các câu, không cần những thứ khác nên bạn không cần diễn đạt!)"

    # Kiểm tra sự tồn tại của auto-generated caption
    has_auto_generated = response.match("kind=asr")

    # Lấy URL của timedtext API
    link_rg = response.match(/https\:\/\/www\.youtube\.com\/api\/timedtext\?.*?"/)

    if link_rg
      timedtext_url = link_rg[0]
      timedtext_url = timedtext_url.gsub('"', '').gsub('\\u0026', '&')
      timedtext_uri = URI(timedtext_url)
      timedtext_response = Net::HTTP.get(timedtext_uri)
      xml_doc = Nokogiri::XML(timedtext_response)
      # quotes = [];
      # full_text = ""
      xml_doc.xpath('//text').each do |text_node|
        start_time = text_node['start']  # Thuộc tính start
        duration = text_node['dur']      # Thuộc tính dur
        text_content = text_node.text.strip  # Nội dung văn bản trong thẻ
        if text_content != "[âm nhạc]"
          # quotes << {
          #   start_time: start_time,
          #   duration: duration,
          #   text: text_content
          # }
          # full_text += text_content + " "
          text += text_content + " "
          # if text_content.length + text.length + prompt.length <= 32000 - 400
          #   text += text_content + " "
          # else
          #   break
          # end
        end
      end
      chunk_size = 5000
      overlap_size = 200

      chunks = split_text(text, chunk_size, overlap_size)

      # In ra kết quả
      chunks.each_with_index do |chunk, index|
        YoutubeParagraph.create(
          ordinal_number: index + 1,
          content_raw: chunk,
          youtube_video_id: youtube_video_id
        )
      end
      # paragraphs = call_api_gemini(prompt)
      # # puts paragraphs
      # # puts "*" * 50
      # matches = paragraphs.scan(/\[(.*?)\]/)

      # # In ra tất cả các nội dung tìm được
      # matches.each do |match|
      #   # puts match[0]
      #   # puts "*" * 50
      # end
      # next_prompt += " với đoạn trích này bạn hãy tìm ra đoạn văn hay nhất và trả lại cho tôi (lưu ý tôi chỉ cần đoạn văn đó, không cần những thứ khác nên bạn không cần diễn đạt!)"
      # result = call_api_gemini(next_prompt)
    else
      # puts "Không tìm thấy timedtext"
    end
  end

  def call_api_gemini(prompt)
      # puts "original prompt: #{prompt}"
      # "AIzaSyCkXcs_Q_JaJGro7NZnLMKapzbn6aeawQo",
      # "AIzaSyAHUbixiqbjBl9kXEilMXpusEb1Ob-cNXc",
      # "AIzaSyAX_hFYdjI52prmmN7GISv6h3_S9wg8kwE",
      # "AIzaSyAc2J8qO6trGfisd-Vz51O4kueeR93T4Hc",
      # "AIzaSyA-hWiI4wnwULLL2ijSqQ0Q_n94oOGumuc",
      # "AIzaSyCVeFAofpnSUoIBDibz9NlGm9Unoi6aLZY",
      # "AIzaSyDbgXHmCJOB6dE0P5uNXDoepghui2fF5m4",
      # "AIzaSyAJ7ELL7SMCGQZaoe2wbuCVPTveJE463k0",
      # "AIzaSyC08uiHofmdtTZi58A_NIMw46oLDtdhYWQ",
      # "AIzaSyC36pleXv9CyfH-gQUtF25WCB5SMe4IfFY",
      # "AIzaSyDuGavRP1M7E7VFan8l88AvxksaPq9ec8c",
      # "AIzaSyALFnOHWpcotkpsi0WmbMx6I3x8T9DvnJ8",
      # "AIzaSyCTX1jxHuN7Co5JVzu1lk7-M-LNA7i9x50",
      # "AIzaSyCtlVU8icEKYdoVzYFbNQzkHpkFnvmXMxY",
      # "AIzaSyC16BSpRhZfVgaMk859avbZQS7RT9V3ZRw",
      # "AIzaSyBQl7SbhOoG4BiVc_lPFMik7ab4oTwEocg",
      # "AIzaSyBHtyOR2CGQoOuXAFCz57UytmflVFCI8xA",
      # "AIzaSyAZOeZljyFILeLqYV44RdnlhgbqEN4rHbA",
      # "AIzaSyB-7bGxSDSUrOsK5PLYeZI0FPEIKPGQJsE",
      # "AIzaSyB3TTrECR4SlFZPZDxpz9jkKHYJjh5yifk",
      # "AIzaSyCo2M8Xhwt9mOzxZ1_zzQ4fJTzQJodOMaI",
      # "AIzaSyBxJA_UCtMci5WmHpfRkax1LHLdZ1fO7IQ",
      # "AIzaSyBVIOHP-MgwMH-3ZmtH05G1BlqS2yc1ULs",
      # "AIzaSyDNEiJhkoIQSdkDTxuFXSQKMPmJtV-XRwM",
      # "AIzaSyDXyOfONSN91uHaXfW-fGq1KhcslV7m4xw",
      # "AIzaSyDyduh_-HqpBypsZdAV-MmOWzWf4gtEue4",
      # "AIzaSyAjMVQ0ohgSu1HdpIfukyf43l8TNnqFqgA",
      # "AIzaSyABc0xoyOwo9Eo3AkzeaSYz2BqF8mnRhfY",
      # "AIzaSyBowcEFj9NQCcestlQBXalEZAtjcsqZ048",
      # "AIzaSyAp5J5QGgCwnnRdiHKeeOeJHdFomukbrUs",
      # "AIzaSyB2xP941VEFHwVuVPuxZosFu7q3Bq7I7gQ",
      # "AIzaSyApKxnC6vHoKjPXW9cLBp1_JhmMGc0gUhw",
      # "AIzaSyAaEIZYdHTLs46I4mPfOAy1IBsQnwaQbus",
      # "AIzaSyCJC7GQJkTkIUfbzIMxJEHD7NU_FXI40Do",
      # "AIzaSyCFkOnXYtjcQXBO0tgA2Khhf37EnjvOvBA",
      # "AIzaSyBIkzaz9VyrxmCAVDKrC9hZlNcdC9e0heo",
      # "AIzaSyAlHyiFSUOCs-psPTy4ThngglorOGZlr9Q",

      ###########
      # open_key_api = [
      #   "AIzaSyBbP55oZ59n0s9SwCvHBaBb0hPwZ6u6tY0",
      #   "AIzaSyCNClX3XU1deqzJQS-l3kkXE8RDGNrkYMI",
      #   "AIzaSyBlu4MDduqBRaPNdW5K3eD3bycsbb0koQU",
      #   "AIzaSyDzXo4P2DnuGDVy9XtWQap3JJnRLyZwGik",
      #   "AIzaSyDq2QZW550sEfyhgaqxFyBK68QntpDMDFw",
      #   "AIzaSyCDX4w-2z2fyTndBDoEmk_tzr_CMx71Wjg",
      #   "AIzaSyC7hwfQEdWgsTboPROaR912M_OGpwo8mC4",
      #   "AIzaSyAC-nH9Y2RpSQoZoG60VXPmFNjgcIx6x5Y",
      #   "AIzaSyBlFeTZbEQt_KRkb-UUPzDDIzZsiZPusCc",
      #   "AIzaSyDyHZw152VFGWBxQNUAQuSGhJad9s1SSM4",
      # ]
      ###############
      open_key_api = [
        "AIzaSyCkXcs_Q_JaJGro7NZnLMKapzbn6aeawQo",
        "AIzaSyAHUbixiqbjBl9kXEilMXpusEb1Ob-cNXc",
        "AIzaSyAX_hFYdjI52prmmN7GISv6h3_S9wg8kwE",
        "AIzaSyAc2J8qO6trGfisd-Vz51O4kueeR93T4Hc",
        "AIzaSyA-hWiI4wnwULLL2ijSqQ0Q_n94oOGumuc",
        "AIzaSyCVeFAofpnSUoIBDibz9NlGm9Unoi6aLZY",
        "AIzaSyDbgXHmCJOB6dE0P5uNXDoepghui2fF5m4",
        "AIzaSyAJ7ELL7SMCGQZaoe2wbuCVPTveJE463k0",
        "AIzaSyC08uiHofmdtTZi58A_NIMw46oLDtdhYWQ",
        "AIzaSyC36pleXv9CyfH-gQUtF25WCB5SMe4IfFY",
        "AIzaSyDuGavRP1M7E7VFan8l88AvxksaPq9ec8c",
        "AIzaSyALFnOHWpcotkpsi0WmbMx6I3x8T9DvnJ8",
        "AIzaSyCTX1jxHuN7Co5JVzu1lk7-M-LNA7i9x50",
        "AIzaSyCtlVU8icEKYdoVzYFbNQzkHpkFnvmXMxY",
        "AIzaSyC16BSpRhZfVgaMk859avbZQS7RT9V3ZRw",
        "AIzaSyBQl7SbhOoG4BiVc_lPFMik7ab4oTwEocg",
        "AIzaSyBHtyOR2CGQoOuXAFCz57UytmflVFCI8xA",
        "AIzaSyAZOeZljyFILeLqYV44RdnlhgbqEN4rHbA",
        "AIzaSyB-7bGxSDSUrOsK5PLYeZI0FPEIKPGQJsE",
        "AIzaSyB3TTrECR4SlFZPZDxpz9jkKHYJjh5yifk",
        "AIzaSyCo2M8Xhwt9mOzxZ1_zzQ4fJTzQJodOMaI",
        "AIzaSyBxJA_UCtMci5WmHpfRkax1LHLdZ1fO7IQ",
        "AIzaSyBVIOHP-MgwMH-3ZmtH05G1BlqS2yc1ULs",
        "AIzaSyDNEiJhkoIQSdkDTxuFXSQKMPmJtV-XRwM",
        "AIzaSyDXyOfONSN91uHaXfW-fGq1KhcslV7m4xw",
        "AIzaSyDyduh_-HqpBypsZdAV-MmOWzWf4gtEue4",
        "AIzaSyAjMVQ0ohgSu1HdpIfukyf43l8TNnqFqgA",
        "AIzaSyABc0xoyOwo9Eo3AkzeaSYz2BqF8mnRhfY",
        "AIzaSyBowcEFj9NQCcestlQBXalEZAtjcsqZ048",
        "AIzaSyAp5J5QGgCwnnRdiHKeeOeJHdFomukbrUs",
        "AIzaSyB2xP941VEFHwVuVPuxZosFu7q3Bq7I7gQ",
        "AIzaSyApKxnC6vHoKjPXW9cLBp1_JhmMGc0gUhw",
        "AIzaSyAaEIZYdHTLs46I4mPfOAy1IBsQnwaQbus",
        "AIzaSyCJC7GQJkTkIUfbzIMxJEHD7NU_FXI40Do",
        "AIzaSyCFkOnXYtjcQXBO0tgA2Khhf37EnjvOvBA",
        "AIzaSyBIkzaz9VyrxmCAVDKrC9hZlNcdC9e0heo",
        "AIzaSyAlHyiFSUOCs-psPTy4ThngglorOGZlr9Q",
      ]
      model = "gemini-2.0-flash"
      request_data = {
        contents: [
          {
            parts: [
              { text: prompt }
            ]
          }
        ],
        safetySettings: [
          { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'BLOCK_NONE' },
          { category: 'HARM_CATEGORY_HARASSMENT', threshold: 'BLOCK_NONE' },
          { category: 'HARM_CATEGORY_HATE_SPEECH', threshold: 'BLOCK_NONE' },
          { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold: 'BLOCK_NONE' }
        ],
        generationConfig: {
          temperature: 0.7,
          topP: 0.8,
          topK: 10,
          responseMimeType: 'text/plain'
        }
      }
      random_index = rand(open_key_api.length)
      api_key = open_key_api[random_index]
      endpoint = "https://generativelanguage.googleapis.com/v1beta/models/#{model}:generateContent?key=#{api_key}"

      response = HTTParty.post(endpoint, body: request_data.to_json, headers: { 'Content-Type' => 'text/plain' })
      begin
        message = JSON.parse(response.body)
        # # puts JSON.pretty_generate(message)
        message_content = ""
        if message['candidates'] && message['candidates'][0] && message['candidates'][0]['content'] && message['candidates'][0]['content']['parts'] && message['candidates'][0]['content']['parts'][0]
          message_content = message['candidates'][0]['content']['parts'][0]['text']
          translated_value = message['candidates'][0]['content']['parts'][0]['text'].gsub(/\n/,' ').gsub(/\r/,' ').gsub('|',' ')
          # language_data << translated_value
          # puts message_content
        else
          # language_data << ""
          puts "//// error ______________"
          puts message
          puts "______________ error ////"
        end

        # current_index = (current_index + 1) % open_key_api.length
        # sleep(1)
      rescue => exception
        puts exception.message
        message_content = exception.message
      end
      return message_content
  end

end
