import schedule
import time
from .logger import setup_logger
from .package_retriever import get_top_packages, get_package_documentation_urls
from .crawler import crawl_url
from .processor import initialize_database, store_page_version, process_and_store_docs
from .config import config

logger = setup_logger(__name__)

def harvest_documentation():
    """
    Orchestrate the entire harvesting process:
      - Initialize the database.
      - Retrieve packages and their documentation URLs.
      - Crawl each documentation URL and store page versions.
      - Process and store deduplicated documentation.
    """
    logger.info("Starting documentation harvesting process.")
    initialize_database()
    
    packages = get_top_packages()
    doc_urls = get_package_documentation_urls(packages)
    
    # In a full implementation, package and documentation URL info should be stored.
    # Here we simulate the process using a placeholder for doc_url_db_id.
    placeholder_doc_url_db_id = 1
    for pkg, urls in doc_urls.items():
        for url in urls:
            logger.info(f"Processing documentation URL: {url} for package: {pkg}")
            crawled_pages = crawl_url(url)
            for page_url, content in crawled_pages.items():
                store_page_version(placeholder_doc_url_db_id, page_url, content)
    
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
