import requests
import time
import re
import hashlib
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from .logger import setup_logger
from .config import config

logger = setup_logger(__name__)

USER_AGENT = config.get('crawler', {}).get('user_agent', "DocumentationHarvesterBot/1.0")

def is_allowed(url):
    """
    Check robots.txt for permission to crawl the given URL.
    """
    parsed_url = urlparse(url)
    robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        allowed = rp.can_fetch(USER_AGENT, url)
        if not allowed:
            logger.info(f"Crawling disallowed by robots.txt at {robots_url} for URL: {url}")
        return allowed
    except Exception as e:
        logger.warning(f"Could not fetch robots.txt from {robots_url}: {e}")
        return True  # Default to allowed if robots.txt cannot be fetched

def extract_links(content, base_url):
    """
    Extract valid HTTP(s) links from HTML content, excluding common download file types.
    """
    soup = BeautifulSoup(content, "html.parser")
    links = set()
    download_exts = re.compile(r'\.(zip|pdf|exe|tar\.gz|tgz|dmg|rar|7z)$', re.IGNORECASE)
    
    for tag in soup.find_all("a", href=True):
        href = tag.get("href")
        if not href.startswith("http"):
            href = urljoin(base_url, href)
        if download_exts.search(href):
            continue
        links.add(href)
    return list(links)

def crawl_url(start_url, depth=0):
    """
    Crawl a starting URL with a breadth-first search up to a maximum depth and page count.
    """
    crawler_config = config.get('crawler', {})
    max_depth = crawler_config.get('max_depth', 1)
    max_pages = crawler_config.get('max_pages', 10)
    request_delay = crawler_config.get('request_delay', 1)
    
    crawled = {}
    queue = [(start_url, depth)]
    pages_explored = 0

    while queue and pages_explored < max_pages:
        current_url, current_depth = queue.pop(0)
        if current_url in crawled or current_depth > max_depth:
            continue

        pages_explored += 1
        logger.info(f"Crawling: {current_url} (depth {current_depth})")
        if not is_allowed(current_url):
            logger.info(f"Skipping disallowed URL: {current_url}")
            continue

        try:
            resp = requests.get(current_url, headers={"User-Agent": USER_AGENT}, timeout=10)
            if resp.status_code == 200:
                content = resp.text
                crawled[current_url] = content
                time.sleep(request_delay)
                if current_depth < max_depth:
                    links = extract_links(content, current_url)
                    for link in links:
                        if link not in crawled:
                            queue.append((link, current_depth + 1))
            else:
                logger.warning(f"Failed to retrieve {current_url}: Status {resp.status_code}")
        except Exception as e:
            logger.error(f"Error retrieving {current_url}: {e}")
    return crawled

def compute_hash(text):
    """
    Compute a SHA1 hash of the given text.
    """
    return hashlib.sha1(text.encode('utf-8')).hexdigest()