import os
import re
import sqlite3
import asyncio
import logging
import asyncpraw
from aiogram import Bot
from utils.queue import Queue
from dotenv import load_dotenv
from dataclasses import dataclass
from datetime import datetime, timezone
from utils.helpers import Schedule, extract_video, download_video, redvid_download_url

load_dotenv()

score_pattern = os.getenv("SCORE_PATTERN", r"\[?\d+\]?\s*-\s*\[?\d+\]?")

logging.basicConfig(
  level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
  format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

db_connection = sqlite3.connect(os.getenv("DB_PATH", "soccer_goals.db"))
task_queue = Queue(db_connection)

@dataclass
class GoalSubmission:
  """Represents a submission for a soccer goal post."""
  submission_id: str
  post_url: str
  post_title: str
  post_flair: str | None

  @classmethod
  def pop(cls):
    task = task_queue.pop()
    return cls(
      task["id"],
      task["url"],
      task["title"],
      None,
    ) if task else None

  def add_to_queue(self, *, is_processed=False):
    task_queue.add(self.submission_id, self.post_url, self.post_title, is_processed=is_processed)

  def is_already_processed(self) -> bool:
    return task_queue.already_processed(self.submission_id)

  def contains_video(self) -> bool:
    if self.post_flair == "media":
      return True
    return any(stream in self.post_url for stream in [
      "stream", "clip", "mixtape", "flixtc", "v.redd", 
      "a.pomfe.co", "kyouko.se", "twitter", "sporttube", "dubz.co", "redvid.io"
    ])
    
  def is_goal_post(self) -> bool:
    """Checks if the post title suggests it's a soccer goal."""
    return re.search(score_pattern, self.post_title) is not None

async def broadcast_goal(bot: Bot, submission: GoalSubmission):
  group_id = os.environ["TELEGRAM_GROUP_ID"]
  channel_id = os.environ["TELEGRAM_CHANNEL_ID"]
  try:
    video_url = submission.post_url if "redvid.io" in submission.post_url else None
    if not video_url:
      video_result = extract_video(submission.post_url)
      video_url = video_result.get('video_url', None)
      downloadable = video_result.get('downloadable', False)
    
    goal_alert_message = "<b>⚽🔥 New Goal Alert! 🔥⚽</b>\n\n" + submission.post_title
    if video_url:
      goal_alert_message += "\n\n<b>🎬 Watch the goal in comment!</b> 👇"
    
    sent_message = await bot.send_message(
      chat_id=channel_id,
      text=goal_alert_message,
      parse_mode="HTML"
    )
    
    if video_url:
      video_file = None
      if downloadable:
        video_file = download_video(video_url, f'temp_goal_video_{submission.submission_id}.mp4')
      
      await asyncio.sleep(5)
      
      updates = await bot.get_updates(offset=-10, limit=10, allowed_updates=["message"])
      sorted_updates = sorted(updates, key=lambda u: u.update_id, reverse=True)
      
      for update in sorted_updates:
        message = update.message
        if message and message.is_automatic_forward and message.forward_from_message_id == sent_message.message_id:
          if video_file:
            with open(video_file, 'rb') as video:
              await bot.send_video(
                chat_id=group_id,
                video=video,
                reply_to_message_id=message.message_id
              )
            os.remove(video_file)
          else:
            await bot.send_video(
              chat_id=group_id,
              video=video_url,
              reply_to_message_id=message.message_id
            )
          break
  except Exception as e:
    logger.exception(
      "%s: failed to broadcast in channel, url: %s, error: %s",
      submission.submission_id,
      video_url,
      str(e)
    )

async def process_submissions(bot: Bot):
  while True:
    submission = GoalSubmission.pop()
    if submission is None:
      return
    logger.info("%s: processing from queue", submission.submission_id)
    await broadcast_goal(bot, submission)

async def fetch_reddit_posts():
  try:
    reddit_client = asyncpraw.Reddit(
      client_id=os.environ["REDDIT_CLIENT_ID"],
      client_secret=os.environ["REDDIT_CLIENT_SECRET"],
      user_agent="r_soccer_goals_bot",
    )
    
    subreddit_name = os.environ.get("REDDIT_SUBREDDIT", "soccer")
    subreddit = await reddit_client.subreddit(subreddit_name)
    print(f"Fetching posts from subreddit: {subreddit_name}")

    async for submission in subreddit.new(limit=10):
      logger.debug(f"Processing submission: {submission.id}")
      post_url = submission.url
      if submission.is_video:
        post_url = redvid_download_url(submission)
      reddit_submission = GoalSubmission(
        submission.id,
        post_url,
        submission.title,
        submission.link_flair_css_class
      )

      if reddit_submission.is_already_processed():
        logger.debug(
          "%s: skipping already processed submission",
          reddit_submission.submission_id,
        )
        continue

      is_goal = reddit_submission.is_goal_post()
      is_video = reddit_submission.contains_video()
      
      is_processed = not (is_goal and is_video)
      reddit_submission.add_to_queue(is_processed=is_processed)
      logging.info("Submission status: is_goal=%s, is_video=%s, processed=%s", is_goal, is_video, is_processed)

    await reddit_client.close()
  except Exception as e:
    print(f"Error: {e}")

async def main():
  bot = Bot(token=os.environ["TELEGRAM_BOT_TOKEN"])
  while True:
    await fetch_reddit_posts()
    await asyncio.sleep(20)
    
    async with asyncio.TaskGroup() as task_group:
      task_group.create_task(process_submissions(bot))
      task_group.create_task(process_submissions(bot))
    
    task_queue.clear()
    await asyncio.sleep(Schedule().refresh_frequency)

if __name__ == "__main__":
  try:
    asyncio.run(main())
  except asyncio.CancelledError:
    print("Task was cancelled!")