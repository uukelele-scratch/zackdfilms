import sqlite3

DB_PATH = "transcriptions.db"
conn = sqlite3.connect(DB_PATH)
c    = conn.cursor()

n = 10

# 1. Query top n most frequent words
c.execute(f"""
    SELECT word, COUNT(*) AS freq
      FROM clips
  GROUP BY LOWER(word)
  ORDER BY freq DESC
     LIMIT {n};
""")

topn = c.fetchall()  # list of (word, freq)

# 2. Print ’em out
print(f"🔥 Top {n} Most Common Words:")
for rank, (word, freq) in enumerate(topn, start=1):
    print(f"{rank}. “{word.strip().lower()}” — {freq} occurrences")

conn.close()