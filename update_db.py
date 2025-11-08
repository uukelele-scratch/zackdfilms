import datetime
def time(): return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Run started at {time()}")

from googleapiclient.discovery import build
from isodate import parse_duration
from alive_progress import alive_bar

from dotenv import load_dotenv; load_dotenv(); import os
API_KEY = os.getenv("GOOGLE_API_KEY")

CHANNEL_USERNAME = 'Zack D. Films'
youtube = build('youtube', 'v3', developerKey=API_KEY)

res = youtube.search().list(q=CHANNEL_USERNAME, type='channel', part='snippet').execute()
channel_id = res['items'][0]['snippet']['channelId']
res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
if not res['items']:
    raise Exception("Channel not found.")

playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
playlist_id


durations = []
video_ids = []
nextPageToken = None

while True:
    res = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50,
        pageToken=nextPageToken
    ).execute()

    for item in res['items']:
        video_ids.append(item['contentDetails']['videoId'])

    nextPageToken = res.get('nextPageToken')
    if not nextPageToken:
        break

with alive_bar(int(len(video_ids) / 50)+1, title="📱 Getting Shorts") as bar:
    # Get durations in batches of 50
    for i in range(0, len(video_ids), 50):
        bar()
        batch_ids = video_ids[i:i+50]
        res = youtube.videos().list(
            part='contentDetails',
            id=','.join(batch_ids)
        ).execute()

        for item in res['items']:
            iso_duration = item['contentDetails']['duration']
            seconds = parse_duration(iso_duration).total_seconds()
            durations.append((item['id'], seconds))

# Analyze shorts

shorts = [(vid, sec) for vid, sec in durations if sec <= 60]

avg_short = sum(sec for _, sec in shorts) / len(shorts)
longest_short = max(shorts, key=lambda x: x[1])
shortest_short = min(shorts, key=lambda x: x[1])

print(f"🎬 Total Shorts: {len(shorts)}")
print(f"📊 Avg Short Duration: {avg_short:.2f}s ({avg_short/60:.2f} min)")
print(f"📈 Longest Short: {longest_short[0]} @ {longest_short[1]:.2f}s")
print(f"📉 Shortest Short: {shortest_short[0]} @ {shortest_short[1]:.2f}s")

import os
import yt_dlp

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
print(len(os.listdir(DOWNLOAD_DIR)), "files exist.")
short_urls = [f"https://www.youtube.com/watch?v={vid}" for vid, _ in shorts]

ydl_opts = {
    'format': 'bestvideo[height<=720]+bestaudio/best[height<=720]',
    'outtmpl': os.path.join(DOWNLOAD_DIR, '%(id)s.%(ext)s'),
    'merge_output_format': 'mp4',
    'noplaylist': True,
    'quiet': True,
    'nocheckcertificate': True
}

downloaded = 0
total = len(shorts)

with yt_dlp.YoutubeDL(ydl_opts) as ydl, alive_bar(len(short_urls), title='📥 Downloading Shorts') as bar:
    for url in short_urls:
        vid = url.split('=')[1]
        out_path = os.path.join(DOWNLOAD_DIR, f"{vid}.mp4")

        # manual skip if file exists
        if os.path.exists(out_path):
            # print(f"🔁 Skipping {vid} (exists)")
            bar()
            downloaded += 1
            continue

        try:
            print(f"⬇️ Downloading {vid}")
            ydl.download([url])
            downloaded += 1
        except Exception as e:
            if "This video may be inappropriate for some users." in str(e):
                print(f"⚠️ {vid}: Video may be inappropriate for some users.")
            else:
                print(f"⚠️ Failed {vid}: {e}")
        bar()

from faster_whisper import WhisperModel
model = WhisperModel("tiny.en")
# tiny, tiny.en, base, base.en, small, small.en, distil-small.en, medium, medium.en, distil-medium.en, large-v1, large-v2, large-v3, large, distil-large-v2, distil-large-v3, large-v3-turbo, or turbo
import sqlite3
import string


DB_PATH = "new.db"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS words (
    id       INTEGER PRIMARY KEY,
    video_id TEXT NOT NULL,
    word     TEXT NOT NULL COLLATE NOCASE,
    start    REAL NOT NULL,
    end      REAL NOT NULL
);
""")
c.execute("CREATE INDEX IF NOT EXISTS idx_word_text ON words(word);")
c.execute("CREATE INDEX IF NOT EXISTS idx_word_video_id ON words(video_id);") 
conn.commit()

c.execute("""
CREATE TABLE IF NOT EXISTS segments (
    id          INTEGER PRIMARY KEY,
    video_id    TEXT NOT NULL,
    segment_text TEXT NOT NULL COLLATE NOCASE,
    start       REAL NOT NULL,
    end         REAL NOT NULL
);
""")
# —  Create an FTS5 “shadow” for fast phrase searches —
c.execute("""
CREATE VIRTUAL TABLE IF NOT EXISTS segments_fts
  USING fts5(
    segment_text,
    video_id    UNINDEXED,
    start       UNINDEXED,
    end         UNINDEXED,
    content='segments',       -- tie it to the real segments table
    content_rowid='id'         -- use the same rowid as segments.id
  );
""")

# Populate it once from any existing segments rows:
c.execute("""
INSERT INTO segments_fts(rowid, segment_text, video_id, start, end)
  SELECT id, segment_text, video_id, start, end FROM segments
  WHERE id NOT IN (SELECT rowid FROM segments_fts);
""")

c.execute("CREATE INDEX IF NOT EXISTS idx_segment_text ON segments(segment_text);") # Crucial for segment lookup
c.execute("CREATE INDEX IF NOT EXISTS idx_segment_video_id ON segments(video_id);")
conn.commit()

files = os.listdir(DOWNLOAD_DIR)
with alive_bar(len(files), title="🗣️ Transcribing Videos") as bar:
    for file in files:
        video_id = os.path.splitext(file)[0]

        # 🚫 Skip if already in DB
        c.execute("SELECT 1 FROM segments WHERE video_id = ? LIMIT 1", (video_id,))
        if c.fetchone():
            bar()  # still tick the bar
            continue

        path = os.path.join(DOWNLOAD_DIR, file)
        segments, info = model.transcribe(path, language="en", word_timestamps=True, append_punctuations="")
        words_to_insert = []
        segments_to_insert = []
        for seg in segments:
            if seg.words:
                segment_start = seg.words[0].start
                segment_end = seg.words[-1].end
                segment_text = seg.text.strip().lower().strip(string.punctuation)

                segments_to_insert.append(
                    (video_id, segment_text, segment_start, segment_end)
                )

                for w in seg.words:

                    clean_word = w.word.strip().lower().strip(string.punctuation)
                    if clean_word:
                        words_to_insert.append(
                            (video_id, clean_word, w.start, w.end)
                        )
            else:
                    print(f"Warning: Segment without words for {video_id} at ~{seg.start:.2f}s: '{seg.text.strip()}'")
                    pass
            
        if segments_to_insert:
            c.executemany(
                "INSERT INTO segments (video_id, segment_text, start, end) VALUES (?, ?, ?, ?)",
                segments_to_insert
            )
            # mirror into FTS table
            c.executemany(
                "INSERT INTO segments_fts(rowid, segment_text, video_id, start, end) VALUES (last_insert_rowid(), ?, ?, ?, ?)",
                segments_to_insert
            )

        if words_to_insert:
            c.executemany(
                "INSERT INTO words (video_id, word, start, end) VALUES (?, ?, ?, ?)",
                words_to_insert
            )

        conn.commit() # Commit after processing each file
        bar()

print(f"✅ All Done!\nUpdated as of {time()}")
