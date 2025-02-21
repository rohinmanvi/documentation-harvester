import sqlite3
import gzip
import hashlib
from .logger import setup_logger
from .config import config

logger = setup_logger(__name__)
DB_PATH = config.get('database', {}).get('path', 'documentation.db')

def initialize_database():
    """
    Initialize the SQLite database with required tables.
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
    Insert a package into the packages table (if not already present) and return its ID.
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
    Insert a documentation URL into the documentation_urls table (if not already present) and return its ID.
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