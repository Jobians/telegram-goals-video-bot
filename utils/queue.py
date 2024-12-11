import sqlite3

class Queue:
  """Handles the database operations for queuing data."""

  def __init__(self, db: sqlite3.Connection):
    self.conn = db
    self.cursor = self.conn.cursor()
    self.cursor.execute("""
      CREATE TABLE IF NOT EXISTS queue (
        id TEXT PRIMARY KEY,
        url TEXT NOT NULL,
        title TEXT NOT NULL,
        processed TIMESTAMP
      )
    """)

  def add(self, submission_id: str, url: str, title: str, *, is_processed=False):
    query = """
      INSERT INTO queue (id, url, title, processed) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """ if is_processed else """
      INSERT INTO queue (id, url, title) VALUES (?, ?, ?)
    """
    self.cursor.execute(query, (submission_id, url, title))
    self.conn.commit()

  def pop(self):
    row = self.cursor.execute(
      "SELECT id, url, title FROM queue WHERE processed IS NULL"
    ).fetchone()
    if not row:
      return None
    self.cursor.execute(
      "UPDATE queue SET processed = CURRENT_TIMESTAMP WHERE id = ?", (row[0],)
    )
    self.conn.commit()
    return {"id": row[0], "url": row[1], "title": row[2]}

  def already_processed(self, submission_id: str) -> bool:
    self.cursor.execute(
      "SELECT 1 FROM queue WHERE id = ? AND processed IS NOT NULL", (submission_id,)
    )
    return self.cursor.fetchone() is not None

  def clear(self):
    self.cursor.execute("""
      DELETE FROM queue
      WHERE processed IS NOT NULL AND processed < DATETIME('now', '-3 days')
    """)
    self.conn.commit()

  def close(self):
    self.conn.close()