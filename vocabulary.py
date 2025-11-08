import sqlite3, os, string
from collections import defaultdict

DB_PATH = "new.db"
conn = sqlite3.connect(DB_PATH)
c    = conn.cursor()

c.execute("SELECT video_id, word, start, end FROM words")
vocab = c.fetchall()
conn.close()

vocab_list = [a[1].strip().lower().strip(string.punctuation) for a in vocab]

DOWNLOAD_DIR = "downloads"

word_index = defaultdict(list)
for video_id, word, start, end in vocab:
    word = word.strip().lower()  # normalize
    video_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp4")
    word_index[word].append({
        "video_path": video_path,
        "start":       start,
        "end":         end
    })

def search_sentence(sentence: str):
    conn = sqlite3.connect(DB_PATH)
    """
    Searches for segments and words from the sentence in the database.

    Prioritizes finding the largest matching segments first using FTS,
    then falls back to individual words.

    Args:
        sentence: The input sentence string.

    Returns:
        A list of dictionaries, each representing a found segment or word
        in the order they appear in the sentence. Each dictionary has keys:
        'type' ('segment' or 'word'), 'text', 'video_id', 'start', 'end'.

    Raises:
        ValueError: If the input sentence is empty or contains no valid words
                    after cleaning.
        Exception: If a word in the sentence cannot be found as part of a
                   segment or as an individual word in the database.
    """
    if not sentence or sentence.isspace():
        # Or return [] if empty input is acceptable without error
        raise ValueError("Input sentence cannot be empty.")

    # 1. Preprocess the sentence: lowercase, strip punctuation, split
    # Create a translation table to remove punctuation
    translator = str.maketrans('', '', string.punctuation)
    # Apply lowercase, strip leading/trailing whitespace, then remove punctuation
    processed_sentence = sentence.strip().lower().translate(translator)
    # Split into words and filter out empty strings
    cleaned_words = [word for word in processed_sentence.split() if word]

    if not cleaned_words:
        raise ValueError("Sentence contains no valid words after cleaning.")

    results = []
    current_word_index = 0
    c = conn.cursor()
    num_words = len(cleaned_words)

    while current_word_index < num_words:
        found_match_in_iteration = False
        # 2. Try to find the largest matching segment starting at current_word_index
        # Iterate from the longest possible phrase down to a single word phrase
        # (We check single words separately later if no multi-word segment matches)
        for length in range(num_words - current_word_index, 0, -1):
            end_index = current_word_index + length
            phrase_words = cleaned_words[current_word_index:end_index]

            # Should only search for segments if length > 1,
            # but FTS might handle single words too. Let's keep it simple:
            # search FTS for any length >= 1. If no segment is found,
            # we'll specifically search the words table later.
            if not phrase_words:
                continue # Should not happen with loop logic

            phrase_to_search = " ".join(phrase_words)
            # Use FTS5 MATCH with double quotes for exact phrase search
            # Note: FTS5 automatically handles tokenization based on spaces etc.
            # Wrapping in quotes ensures the sequence is matched.
            fts_query = f'"{phrase_to_search}"'

            # print(f"DEBUG: Trying segment search for: {fts_query}") # Optional debug

            try:
                # Query the FTS table first
                c.execute(
                    """
                    SELECT sfts.video_id, sfts.start, sfts.end, sfts.segment_text
                    FROM segments_fts AS sfts
                    WHERE sfts.segment_text MATCH ?
                    LIMIT 1
                    """,
                    (fts_query,)
                )
                segment_match = c.fetchone()
            except sqlite3.OperationalError as e:
                 # Handle potential FTS query errors, e.g., malformed query
                 print(f"Warning: FTS Query Error for '{fts_query}': {e}. Skipping phrase.")
                 # Treat as no match found for this phrase length
                 segment_match = None


            if segment_match:
                # print(f"DEBUG: Found segment: {segment_match}") # Optional debug
                video_id, start, end, segment_text = segment_match
                results.append({
                    "type": "segment",
                    "text": segment_text, # Use text from DB for correct representation
                    "video_id": video_id,
                    "start": start,
                    "end": end
                })
                current_word_index = end_index # Move index past the found segment
                found_match_in_iteration = True
                break # Found the largest segment starting here, move to next part

        # 3. If no segment (multi-word or single-word) was found via FTS starting at current_word_index
        if not found_match_in_iteration:
            word_to_search = cleaned_words[current_word_index]
            # print(f"DEBUG: Trying word search for: {word_to_search}") # Optional debug

            c.execute(
                """
                SELECT video_id, start, end, word
                FROM words
                WHERE word = ? COLLATE NOCASE -- Ensure case-insensitivity just in case
                LIMIT 1
                """,
                (word_to_search,)
            )
            word_match = c.fetchone()

            if word_match:
                # print(f"DEBUG: Found word: {word_match}") # Optional debug
                video_id, start, end, word_text = word_match
                results.append({
                    "type": "word",
                    "text": word_text, # Use the text from DB
                    "video_id": video_id,
                    "start": start,
                    "end": end
                })
                current_word_index += 1
                found_match_in_iteration = True
            else:
                # 4. If neither segment nor word found, raise error
                original_word = "???"
                # Try to find the original word before cleaning (best effort)
                # This requires tracking original words alongside cleaned ones,
                # or re-parsing, which adds complexity. Let's just use the cleaned word for the error.
                # A more robust solution might store original word mapping if needed.
                raise Exception(f"Word '{word_to_search}' (from original sentence position {current_word_index+1}) not found in the database.")

    return results

def search_segments(sentence, db_path="new.db"):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Normalize sentence
    normalized = sentence.strip().lower().translate(str.maketrans('', '', string.punctuation))

    # We'll try finding the longest phrases first (split then rejoin)
    words = normalized.split()
    found_segments = []
    used_indices = set()

    # Try n-grams, longest first
    for n in range(len(words), 0, -1):
        for i in range(len(words) - n + 1):
            if any(j in used_indices for j in range(i, i + n)):
                continue  # already used in a longer match

            phrase = ' '.join(words[i:i + n])
            c.execute("""
                SELECT video_id, start, end FROM segments_fts
                WHERE segment_text MATCH ?
                LIMIT 1
            """, (phrase,))
            row = c.fetchone()
            if row:
                found_segments.append({
                    "phrase": phrase,
                    "video_path": os.path.join(DOWNLOAD_DIR, f"{row[0]}.mp4"),
                    "start": row[1],
                    "end": row[2],
                })
                used_indices.update(range(i, i + n))

    conn.close()
    return found_segments, [w for i, w in enumerate(words) if i not in used_indices]

def list_all_segments(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
      SELECT id, video_id, segment_text, start, end
        FROM segments
       ORDER BY video_id, start
    """)
    rows = c.fetchall()
    conn.close()
    # pretty-print
    for seg_id, vid, text, s, e in rows:
        path = os.path.join(DOWNLOAD_DIR, f"{vid}.mp4")
        print(f"[{seg_id}] {vid} ({s:.2f}s–{e:.2f}s): “{text}” → {path}")


if __name__ == "__main__":
    results = search_sentence("this means that")
    for r in results:
        print(r["text"].ljust(15), r["type"])