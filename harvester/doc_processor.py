import sqlite3
import gzip
from .logger import setup_logger
from .config import config
from .text_processing import extract_text_from_html, gopher_quality_filter, minhash_deduplication
import nltk
nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)

logger = setup_logger(__name__)
DB_PATH = config.get('database', {}).get('path', 'documentation.db')

def process_and_store_docs():
    """
    Process raw page versions: extract text, apply quality filters, deduplicate,
    and store the processed documents in the database.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, raw_content FROM page_versions")
    rows = cursor.fetchall()
    processed_texts = []
    page_ids = []

    for page_version_id, compressed_content in rows:
        try:
            raw_html = gzip.decompress(compressed_content).decode('utf-8')
        except Exception as e:
            logger.error(f"Error decompressing page {page_version_id}: {e}")
            continue

        text = extract_text_from_html(raw_html)
        if not gopher_quality_filter(text):
            logger.info(f"Page {page_version_id} did not pass quality filter.")
            continue

        processed_texts.append(text)
        page_ids.append(page_version_id)

    unique_texts = minhash_deduplication(processed_texts, num_hashes=50, num_bands=10, ngram_length=3, jaccard_threshold=0.8)
    logger.info(f"Deduplicated {len(processed_texts) - len(unique_texts)} out of {len(processed_texts)} texts.")

    for page_version_id, text in zip(page_ids, processed_texts):
        if text in unique_texts:
            compressed_text = gzip.compress(text.encode('utf-8'))
            cursor.execute('''
                INSERT INTO processed_docs (page_version_id, processed_text)
                VALUES (?, ?)
            ''', (page_version_id, compressed_text))
    conn.commit()
    conn.close()
    logger.info("Processing and storing complete.")