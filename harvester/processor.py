import sqlite3
import gzip
import hashlib
import re
import unicodedata
import nltk
from nltk.tokenize import word_tokenize
from nltk.util import ngrams
from collections import defaultdict
from hashlib import sha1
import resiliparse.extract.html2text as html2text
from .logger import setup_logger
from .config import config

nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)

logger = setup_logger(__name__)

DB_PATH = config.get('database', {}).get('path', 'documentation.db')

def initialize_database():
    """
    Initialize the SQLite database with the required tables.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS packages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS documentation_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_id INTEGER,
            url TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(package_id, url),
            FOREIGN KEY (package_id) REFERENCES packages(id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS page_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_url_id INTEGER NOT NULL,
            page_url TEXT NOT NULL,
            version INTEGER NOT NULL,
            raw_content BLOB,
            content_hash TEXT,
            retrieved_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            change_summary TEXT,
            UNIQUE(doc_url_id, page_url, version),
            FOREIGN KEY (doc_url_id) REFERENCES documentation_urls(id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_docs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_version_id INTEGER,
            processed_text BLOB,
            processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(page_version_id) REFERENCES page_versions(id)
        )
    ''')
    conn.commit()
    conn.close()

def store_package(package_name):
    """
    Insert a package into the packages table (if it doesn't already exist)
    and return its ID.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO packages (name) VALUES (?)", (package_name,))
    conn.commit()
    cursor.execute("SELECT id FROM packages WHERE name = ?", (package_name,))
    pkg_id = cursor.fetchone()[0]
    conn.close()
    logger.info(f"Stored/Retrieved package '{package_name}' with id: {pkg_id}")
    return pkg_id

def store_doc_url(package_id, url):
    """
    Insert a documentation URL into the documentation_urls table (if it doesn't exist)
    and return its ID.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO documentation_urls (package_id, url) VALUES (?, ?)", (package_id, url))
    conn.commit()
    cursor.execute("SELECT id FROM documentation_urls WHERE package_id = ? AND url = ?", (package_id, url))
    doc_url_id = cursor.fetchone()[0]
    conn.close()
    logger.info(f"Stored/Retrieved documentation URL '{url}' with id: {doc_url_id} for package id: {package_id}")
    return doc_url_id

def store_page_version(doc_url_db_id, page_url, content):
    """
    Store a new version of a documentation page if its content has changed.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    compressed_content = gzip.compress(content.encode('utf-8'))
    content_hash = hashlib.sha1(content.encode('utf-8')).hexdigest()

    cursor.execute('''
        SELECT id, version, content_hash FROM page_versions
        WHERE doc_url_id = ? AND page_url = ?
        ORDER BY version DESC LIMIT 1
    ''', (doc_url_db_id, page_url))
    row = cursor.fetchone()

    if row:
        existing_hash = row[2]
        version = row[1]
        if content_hash == existing_hash:
            logger.info(f"No changes for page: {page_url}")
            conn.close()
            return
        else:
            new_version = version + 1
            change_summary = "Content changed"
    else:
        new_version = 1
        change_summary = "Initial version"

    cursor.execute('''
        INSERT INTO page_versions (doc_url_id, page_url, version, raw_content, content_hash, change_summary)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (doc_url_db_id, page_url, new_version, compressed_content, content_hash, change_summary))
    conn.commit()
    conn.close()
    logger.info(f"Stored version {new_version} for page: {page_url}")

def extract_text_from_html(html_string):
    """
    Extract plain text from HTML using resiliparse.
    """
    return html2text.extract_plain_text(html_string)

def gopher_quality_filter(text):
    """
    Determine whether the extracted text meets quality thresholds.
    """
    words = word_tokenize(text)
    num_words = len(words)
    if num_words < 50 or num_words > 100000:
        return False
    mean_word_length = sum(len(word) for word in words) / num_words if num_words > 0 else 0
    if mean_word_length < 3 or mean_word_length > 10:
        return False
    lines = text.split('\n')
    ellipsis_count = sum(1 for line in lines if line.endswith('...'))
    ellipsis_ratio = ellipsis_count / len(lines) if lines else 0
    if ellipsis_ratio > 0.3:
        return False
    alphabetic_words_ratio = sum(1 for word in words if any(char.isalpha() for char in word)) / num_words if num_words > 0 else 0
    if alphabetic_words_ratio < 0.7:
        return False
    return True

def normalize_text(text):
    """
    Normalize text for deduplication purposes.
    """
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def compute_ngrams(text, n):
    tokens = word_tokenize(text)
    return list(ngrams(tokens, n))

def minhash(ngram_list, num_hashes):
    return [min([int(sha1(f"{h}{ngram}".encode()).hexdigest(), 16) for ngram in ngram_list]) for h in range(num_hashes)]

def lsh(signatures, num_bands):
    bands = defaultdict(list)
    rows_per_band = len(signatures[0]) // num_bands
    for doc_id, sig in enumerate(signatures):
        for i in range(num_bands):
            band_signature = tuple(sig[i * rows_per_band: (i + 1) * rows_per_band])
            bands[band_signature].append(doc_id)
    return bands

def jaccard_similarity(set1, set2):
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0

def minhash_deduplication(processed_texts, num_hashes, num_bands, ngram_length, jaccard_threshold):
    normalized_docs = [normalize_text(text) for text in processed_texts]
    ngram_sets = [set(compute_ngrams(doc, ngram_length)) for doc in normalized_docs]
    minhash_signatures = [minhash(list(ngrams_set), num_hashes) for ngrams_set in ngram_sets]
    
    bands = lsh(minhash_signatures, num_bands)
    
    candidate_pairs = set()
    for band in bands.values():
        if len(band) > 1:
            for i in range(len(band)):
                for j in range(i + 1, len(band)):
                    candidate_pairs.add((band[i], band[j]))
    
    to_remove = set()
    for i, j in candidate_pairs:
        if jaccard_similarity(ngram_sets[i], ngram_sets[j]) >= jaccard_threshold:
            to_remove.add(j)
    
    unique_texts = [text for idx, text in enumerate(processed_texts) if idx not in to_remove]
    return unique_texts

def process_and_store_docs():
    """
    Process raw page versions: extract text, apply quality filters, deduplicate, and store in processed_docs.
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
