# Documentation Harvester

A robust system to automatically harvest documentation from popular programming packages and libraries to create and maintain a dataset for AI training. The system retrieves documentation URLs, crawls pages (while respecting robots.txt), processes and deduplicates the content, and stores version-controlled documentation in an SQLite database.

## Features

- **Identify and Track Packages:** Retrieve popular packages from sources (currently PyPI; extendable to other languages).
- **Documentation Retrieval:** Crawl documentation pages while respecting source websites' terms via dedicated robots.txt parsing.
- **Version Control:** Store and version-control documentation changes.
- **Content Processing:** Preprocess, clean, and deduplicate documentation content.
- **Configurable Scheduling:** Run harvesting tasks periodically (or on-demand).
- **Enhanced Logging:** Detailed logging with configurable log levels.
- **Centralized Configuration:** Easily tune settings via a YAML configuration file.

## Repository Structure

```
documentation-harvester/
├── README.md
├── LICENSE
├── requirements.txt
├── config.yaml
├── harvester/
│   ├── __init__.py
│   ├── config.py
│   ├── logger.py
│   ├── package_retriever.py
│   ├── crawler.py
│   ├── processor.py
│   └── scheduler.py
└── scripts/
    ├── run_once.py
    └── run_scheduler.py
```

## Installation

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/yourusername/documentation-harvester.git
   cd documentation-harvester
   ```

2. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Edit the `config.yaml` file to adjust API endpoints, crawl settings, logging levels, scheduling intervals, and database paths.

## Running the Harvester

- **Run Once (one-time execution):**
  ```bash
  python scripts/run_once.py
  ```

- **Run Scheduler (periodic execution):**
  ```bash
  python scripts/run_scheduler.py
  ```

## Using in Google Colab

In a Colab notebook, run the following commands to clone and run the repository:

```python
!git clone https://github.com/yourusername/documentation-harvester.git
%cd documentation-harvester
!pip install -r requirements.txt
!python scripts/run_once.py  # or use run_scheduler.py for periodic execution
```
