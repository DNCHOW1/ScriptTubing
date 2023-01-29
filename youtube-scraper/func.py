import functions_framework
import os
import re
import yt_dlp
import firebase_admin

from google.cloud import storage
from firebase_admin import firestore, credentials
from yt_dlp.downloader import get_suitable_downloader

ydl_opts = {
    "writesubtitles": True,
    "writeautomaticsub": True,
    "subtitleslangs": ["en"],
    "subtitlesformat": "ttml"
}

ydl = None
db = None
transcripts = None
videos = None
storage_client = None
bucket = None
idRegex = re.compile(r"watch\?v=(.*)")

@functions_framework.http
def hello_http(request):
    global db, ydl, transcripts, videos, storage_client, bucket

    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, HEAD, POST',
            'Access-Control-Allow-Headers': 'Content-Type'
        }

        return ('', 204, headers)
    
    URLS = request.json.get("urls")
    IDS = []
    for URL in URLS:
      # Check to see if url id is in the db already, if it is then "continue"
      if not (match := re.search(idRegex, URL)):
          continue

      videoId = match.group(1)
      IDS.append(videoId)
      if not db:
          # Use a service account.
          cred = credentials.Certificate("./...")

          app = firebase_admin.initialize_app(cred)
          db = firestore.client()
          transcripts = db.collection(u"transcripts")
          videos = db.collection(u"videos")

      if videos.document(u"{}".format(videoId)).get().exists:
          continue

      if not ydl:
          ydl = yt_dlp.YoutubeDL(ydl_opts)

      info = ydl.extract_info(URL, download=False)

      info['requested_subtitles'] = ydl.process_subtitles(
          info['id'], info.get('subtitles'), info.get('automatic_captions')
      )

      subtitleInfo = info.get('requested_subtitles', {})
      if not subtitleInfo: continue
      subtitleInfo = subtitleInfo.get("en")
      subtitleInfo.setdefault('http_headers', 
              {
                  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36",
                  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                  "Accept-Language": "en-us,en;q=0.5",
                  "Sec-Fetch-Mode": "navigate"
              })
      newSubInfo = dict(subtitleInfo)
      fd = get_suitable_downloader(subtitleInfo, ydl.params)(ydl, ydl.params)
      fd.download(f"subtitle.ttml", newSubInfo, True)

      if not storage_client:
          storage_client = storage.Client()
          bucket = storage_client.bucket("youtube-subtitles")
      blob = bucket.blob(f"{videoId}.ttml")

      with open(f"subtitle.ttml", "a") as f:
        f.write(f"{videoId}\n")
        f.write(f"{info.get('categories')}\n")
        f.write(f"{info.get('channel')}\n")

      # Upload data from the stream to your bucket.
      with open(f"subtitle.ttml", "rb") as f:
        blob.upload_from_file(f)
      print("uploaded")

      os.remove("subtitle.ttml")

    #   # Write the video to our database
      videos.document(u"{}".format(videoId)).set({
          u"title": info.get("fulltitle"),
          u"url": info.get("webpage_url"),
          u"categories": info.get("categories"),
          u"upload_date": info.get("upload_date"),
          u"like_count": info.get("like_count"),
          u"view_count": info.get("view_count"),
          u"duration": info.get("duration"),
          u"channel": info.get("channel"),
          u"thumbnail": info.get("thumbnail"),
          u"video_id": videoId
      })

    return (json.dumps({"ids": IDS}), 200, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
    })
