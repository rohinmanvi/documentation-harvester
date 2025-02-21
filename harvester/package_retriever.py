import requests
from .logger import setup_logger
from .config import config

logger = setup_logger(__name__)

def get_top_packages():
    """Retrieve the list of popular packages from the configured source (currently PyPI)."""
    packages_config = config.get('packages', {})
    source = packages_config.get('source', 'pypi')
    if source == 'pypi':
        top_packages_url = packages_config.get('top_packages_url')
        top_n = packages_config.get('top_n', 20)
        try:
            response = requests.get(top_packages_url)
            response.raise_for_status()
            data = response.json()
            top_packages = [pkg['project'] for pkg in data.get('rows', [])[:top_n]]
            logger.info(f"Retrieved top {len(top_packages)} packages from PyPI.")
            return top_packages
        except Exception as e:
            logger.error(f"Error retrieving top packages: {e}")
            return []
    else:
        logger.error("Unsupported package source.")
        return []

def extract_doc_url(package_info):
    """
    Extract documentation URLs from PyPI package metadata.
    """
    urls = []
    doc_url = package_info.get("docs_url")
    if doc_url:
        urls.append(doc_url)
    
    project_urls = package_info.get("project_urls", {})
    for key, url in project_urls.items():
        if "doc" in key.lower() or "readthedocs" in url.lower():
            urls.append(url)
    
    homepage = package_info.get("home_page")
    if homepage and ("docs" in homepage.lower() or "readthedocs" in homepage.lower()):
        urls.append(homepage)
    
    return list(set(urls))

def get_package_documentation_urls(packages):
    """
    For each package, retrieve its documentation URLs from PyPI.
    """
    doc_urls = {}
    for pkg in packages:
        pypi_url = f"https://pypi.org/pypi/{pkg}/json"
        try:
            response = requests.get(pypi_url)
            if response.status_code == 200:
                pkg_data = response.json().get("info", {})
                urls = extract_doc_url(pkg_data)
                doc_urls[pkg] = urls
            else:
                doc_urls[pkg] = []
                logger.warning(f"Failed to retrieve package info for {pkg}: Status {response.status_code}")
        except Exception as e:
            logger.error(f"Error retrieving package {pkg}: {e}")
            doc_urls[pkg] = []
    return doc_urls