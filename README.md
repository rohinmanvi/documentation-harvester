# Documentation Harvester

## Overview

The Documentation Harvester automatically tracks popular packages (currently from PyPI), retrieves their documentation URLs, crawls the linked pages (while respecting robots.txt), processes and deduplicates the content, and stores version-controlled documentation in an SQLite database. This pipeline is designed for periodic execution and easy extension to support other languages and data sources.

## Usage

### One-Time Run

To run the harvester once:

```bash
python scripts/run_once.py
```

### Scheduled Execution

To run the harvester periodically using a built-in scheduler:

```bash
python scripts/run_scheduler.py
```

In Google Colab, clone and run as follows:

```python
!git clone https://github.com/yourusername/documentation-harvester.git
%cd documentation-harvester
!pip install -r requirements.txt
!python scripts/run_once.py  # or run_scheduler.py for periodic harvesting
```

## Database Schema

The system uses an SQLite database (`documentation.db`) with the following tables:

- **packages**  
  Stores package names (unique per package).

- **documentation_urls**  
  Maps each package to one or more documentation URLs (unique per package/URL pair).

- **page_versions**  
  Stores crawled pages (with versioning) by associating each page with a documentation URL. A SHA1 hash is computed for each page to store only new versions when content changes.

- **processed_docs**  
  Contains processed and deduplicated text extracted from page versions.

## Potential Improvements

- **Extended Language Support:**  
  Add new package retrieval modules for other ecosystems (e.g., npm, RubyGems).

- **Enhanced Crawling:**  
  Use asynchronous HTTP requests for faster crawling and improved error handling.

- **Robust Scheduling:**  
  Integrate with more advanced schedulers (e.g., Airflow, APScheduler) for production-level orchestration.

- **Improved Quality Assessment:**  
  Refine text extraction, quality filters, and deduplication strategies.

- **Monitoring & Alerting:**  
  Implement real-time logging aggregation, performance metrics, and error notifications.

- **Scalable Storage:**  
  Consider migrating from SQLite to PostgreSQL or another robust database solution for large-scale deployments.
