import json
import sqlite3
import aiohttp
import logging
import http.client
import urllib.parse
import requests
from bs4 import BeautifulSoup
from http import HTTPStatus
from yt_dlp import YoutubeDL
from datetime import UTC, datetime
from utils.playwright import playwright_download_url

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class Schedule:
  """
  Schedule is a class with helpers to determine the frequency with which
  we should query reddit for new content
  """

  def __init__(self):
    self.now = datetime.now(tz=UTC)

  @property
  def is_saturday(self):
    return self.now.weekday() == 5

  @property
  def is_sunday(self):
    return self.now.weekday() == 6

  @property
  def is_afternoon(self):
    return self.now.hour > 11 and self.now.hour < 17

  @property
  def is_evening(self):
    return self.now.hour > 17 and self.now.hour < 23

  @property
  def is_night(self):
    return self.now.hour > 1 and self.now.hour < 12

  @property
  def refresh_frequency(self) -> int:
    # during the night there is no Serie A
    if self.is_night:
      return 60 * 5
    if self.is_evening:
      return 60
    if (self.is_saturday or self.is_sunday) and (
      self.is_afternoon or self.is_evening
    ):
      return 60
    return 60 * 2
  
def redvid_download_url(submission):
  if submission.is_video:
    post_id = submission.id
    video_url = submission.media['reddit_video']['fallback_url']
    audio_url = f"{video_url.rsplit('/', 1)[0]}/DASH_AUDIO_128.mp4"
    
    base_url = "redvid.io"
    token = {
      "video_url": video_url,
      "audio_url": audio_url,
      "id": post_id
    }
    
    json_token = json.dumps(token)
    encoded_token = urllib.parse.quote(json_token)
    download_url = f"/download-link?token={encoded_token}"
    
    conn = http.client.HTTPSConnection(base_url)
    conn.request("GET", download_url)
    
    response = conn.getresponse()
    if response.status == 200:
      data = json.loads(response.read().decode())
      if data.get("success") and "url" in data:
        return f"https://{base_url}{data['url']}"
  
    return None

def extract_with_ytdlp(url):
  ydl_opts = {
    'quiet': True,
    'format': 'best',
  }
  with YoutubeDL(ydl_opts) as ydl:
    try:
      info = ydl.extract_info(url, download=False)
      return info['url']
    except Exception as e:
      logging.error(f"yt-dlp failed: {e}")
      return None

def extract_custom(url):
  headers = {"User-Agent": "Mozilla/5.0"}
  try:
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    video_tag = soup.find('video')
    if video_tag and video_tag.get('src'):
      return video_tag.get('src')

    source_tag = soup.find('video').find('source') if soup.find('video') else None
    if source_tag and source_tag.get('src'):
      return source_tag.get('src')

    object_tag = soup.find('object')
    if object_tag and object_tag.get('data'):
      return object_tag.get('data')

    embed_tag = soup.find('embed')
    if embed_tag and embed_tag.get('src'):
      return embed_tag.get('src')

    logging.error("No video found.")
    return None
  except Exception as e:
    logging.error(f"Custom extraction failed: {e}")
    return None

def is_video_downloadable(video_url):
  try:
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.head(video_url, headers=headers, allow_redirects=True)
    if response.status_code == 200:
      content_disposition = response.headers.get('Content-Disposition', '')
      return 'attachment' in content_disposition.lower()
    else:
      logging.error(f"Failed to check video URL: {video_url}, status code: {response.status_code}")
      return False
  except Exception as e:
    logging.error(f"Error checking if video is downloadable: {e}")
    return False

def extract_video(url):
  video_link = extract_with_ytdlp(url)
  if video_link:
    downloadable = is_video_downloadable(video_link)
    return {"video_url": video_link, "downloadable": downloadable}

  custom_link = playwright_download_url(url)
  if custom_link:
    downloadable = is_video_downloadable(custom_link)
    return {"video_url": custom_link, "downloadable": downloadable}

  logging.error("Both yt-dlp and custom extraction failed.")
  return None

def download_video(video_url, filename):
  response = requests.get(video_url, stream=True)
  with open(filename, 'wb') as f:
    for chunk in response.iter_content(chunk_size=1024):
      if chunk:
        f.write(chunk)
  return filename