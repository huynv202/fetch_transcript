import sys
import json
import os
import re
import html
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from http.cookiejar import MozillaCookieJar
from requests import Session
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
from youtube_transcript_api.proxies import GenericProxyConfig

SKIP_TEXTS = {"[music]", "[âm nhạc]"}
DEFAULT_LANGS = ["vi", "en"]


def build_proxy_config():
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")

    if not http_proxy and not https_proxy:
        return None

    return GenericProxyConfig(http_url=http_proxy, https_url=https_proxy)


def build_http_client():
    session = Session()
    session.headers.update({"Accept-Language": "en-US,en;q=0.9,vi;q=0.8"})

    cookie_file = os.getenv("YOUTUBE_COOKIE_FILE")
    if cookie_file:
      cookie_jar = MozillaCookieJar()
      cookie_jar.load(cookie_file, ignore_discard=True, ignore_expires=True)
      session.cookies = cookie_jar

    return session


def normalize_segments(items, language):
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
        "video_id": None,
        "language": language,
        "segments": segments,
        "transcript": "\n".join(full_text)
    }


def fetch_with_youtube_transcript_api(video_id, langs):
    ytt_api = YouTubeTranscriptApi(
        proxy_config=build_proxy_config(),
        http_client=build_http_client()
    )
    transcript_list = ytt_api.list(video_id)

    transcript = None
    try:
        transcript = transcript_list.find_transcript(langs)
    except Exception:
        transcript = transcript_list.find_generated_transcript(langs)

    data = transcript.fetch()

    result = normalize_segments([
        {
            "start": item.start,
            "duration": item.duration,
            "text": item.text
        }
        for item in data
    ], transcript.language_code)
    result["video_id"] = video_id
    return result


def fetch_url(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9,vi;q=0.8"
        }
    )
    proxy_handler = urllib.request.ProxyHandler({
        "http": os.getenv("HTTP_PROXY") or os.getenv("http_proxy"),
        "https": os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    })
    handlers = [proxy_handler]

    cookie_file = os.getenv("YOUTUBE_COOKIE_FILE")
    if cookie_file:
        cookie_jar = MozillaCookieJar()
        cookie_jar.load(cookie_file, ignore_discard=True, ignore_expires=True)
        handlers.append(urllib.request.HTTPCookieProcessor(cookie_jar))

    opener = urllib.request.build_opener(*handlers)

    with opener.open(request, timeout=20) as response:
        return response.read().decode("utf-8", errors="ignore")


def extract_caption_tracks(video_id):
    watch_html = fetch_url(f"https://www.youtube.com/watch?v={video_id}")
    match = re.search(r'"captionTracks":(\[.*?\])', watch_html)
    if not match:
        raise ValueError("No caption tracks found on watch page")
    return json.loads(html.unescape(match.group(1)))


def pick_caption_track(caption_tracks, langs):
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


def parse_caption_xml(xml_text):
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


def parse_caption_json(json_text):
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


def fetch_with_caption_track_fallback(video_id, langs):
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

    result = normalize_segments(items, track.get("languageCode", langs[0]))
    result["video_id"] = video_id
    return result


def fetch_transcript(video_id, lang="vi"):
    langs = [lang] + [item for item in DEFAULT_LANGS if item != lang]
    errors = []

    try:
        return fetch_with_youtube_transcript_api(video_id, langs)
    except TranscriptsDisabled:
        errors.append("Transcripts are disabled for this video")
    except NoTranscriptFound:
        errors.append("No transcript found")
    except Exception as e:
        errors.append(str(e))

    try:
        return fetch_with_caption_track_fallback(video_id, langs)
    except Exception as e:
        errors.append(f"Caption track fallback failed: {e}")

    return {"error": "\n".join(errors)}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Missing VIDEO_ID"}))
        sys.exit(1)

    video_id = sys.argv[1]
    lang = sys.argv[2] if len(sys.argv) > 2 else "vi"

    result = fetch_transcript(video_id, lang)
    print(json.dumps(result, ensure_ascii=False, indent=2))
