import schedule
import time
from .logger import setup_logger
from .package_retriever import get_top_packages, get_package_documentation_urls
from .crawler import crawl_url
from .db import initialize_database, store_package, store_doc_url, store_page_version
from .doc_processor import process_and_store_docs
from .config import config

logger = setup_logger(__name__)

def harvest_documentation():
    """
    Orchestrate the entire harvesting process:
      - Initialize the database.
      - Retrieve packages and their documentation URLs.
      - Store package and URL info.
      - Crawl each documentation URL and store page versions.
      - Process and store deduplicated documentation.
    """
    logger.info("Starting documentation harvesting process.")
    initialize_database()
    
    packages = get_top_packages()
    doc_urls = get_package_documentation_urls(packages)
    
    for pkg, urls in doc_urls.items():
        pkg_id = store_package(pkg)
        for url in urls:
            doc_url_id = store_doc_url(pkg_id, url)
            logger.info(f"Processing documentation URL: {url} for package: {pkg}")
            crawled_pages = crawl_url(url)
            for page_url, content in crawled_pages.items():
                store_page_version(doc_url_id, page_url, content)
    
    process_and_store_docs()
    logger.info("Documentation harvesting process complete.")

def start_scheduler():
    """
    Start a scheduler that runs the harvesting process periodically.
    """
    scheduler_config = config.get('scheduler', {})
    interval = scheduler_config.get('interval_minutes', 60)
    schedule.every(interval).minutes.do(harvest_documentation)
    logger.info(f"Scheduler started. Harvesting every {interval} minutes.")
    
    while True:
        schedule.run_pending()
        time.sleep(1)