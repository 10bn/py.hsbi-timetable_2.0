import logging
import os
from typing import Dict

from webdav3.client import Client

from utils import load_config
from logger import setup_logger

# Set up the logger
setup_logger()
logger = logging.getLogger(__name__)


def initialize_client(url: str, credentials: Dict[str, str], key: str) -> Client:
    """
    Initialize a WebDAV client.

    Args:
        url (str): WebDAV URL.
        credentials (Dict[str, str]): Dictionary containing 'username' and 'password'.
        key (str): Identifier for logging purposes.

    Returns:
        Client: Configured WebDAV client.

    Raises:
        Exception: If client initialization fails.
    """
    options = {
        "webdav_hostname": url,
        "webdav_login": credentials.get("username"),
        "webdav_password": credentials.get("password"),
        "verbose": True,
    }

    try:
        client = Client(options)
        client.verify = True  # Set to False to skip SSL verification if needed
        logger.debug(f"Initialized WebDAV client for '{key}'")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize WebDAV client for '{key}': {e}")
        raise


def list_files(client: Client, key: str) -> list:
    """
    Retrieve a list of files from the WebDAV server.

    Args:
        client (Client): Configured WebDAV client.
        key (str): Identifier for logging purposes.

    Returns:
        list: List of file paths.

    Raises:
        Exception: If listing files fails.
    """
    try:
        files = client.list()
        logger.info(f"Retrieved {len(files)} files from '{key}'")
        return files
    except Exception as e:
        logger.error(f"Failed to list files for '{key}': {e}")
        raise


def download_file(client: Client, remote_path: str, local_path: str, dry_run: bool, key: str) -> None:
    """
    Download a single file from the WebDAV server.

    Args:
        client (Client): Configured WebDAV client.
        remote_path (str): Path to the remote file.
        local_path (str): Path where the file will be saved locally.
        dry_run (bool): If True, simulate the download without performing it.
        key (str): Identifier for logging purposes.

    Raises:
        Exception: If downloading the file fails.
    """
    if dry_run:
        logger.info(f"Dry run enabled. Skipping download of '{remote_path}' to '{local_path}'.")
        return

    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        client.download_sync(remote_path=remote_path, local_path=local_path)
        logger.info(f"Successfully downloaded '{remote_path}' to '{local_path}'")
    except Exception as e:
        logger.error(f"Failed to download file '{remote_path}' from '{key}': {e}")
        raise


def process_url(key: str, url: str, credentials: Dict[str, str], keyword: str, dry_run: bool, output_path: str) -> None:
    """
    Process a single WebDAV URL: list files and download relevant PDFs based on the keyword.

    Args:
        key (str): Identifier for the URL.
        url (str): WebDAV URL.
        credentials (Dict[str, str]): Credentials for accessing the WebDAV server.
        keyword (str): Keyword to filter PDF files.
        dry_run (bool): If True, simulate downloads without performing them.
        output_path (str): Base directory to save downloaded files.
    """
    logger.info(f"Processing URL for '{key}': {url}")

    if not isinstance(keyword, str):
        logger.error(f"Keyword for '{key}' must be a string.")
        return

    keyword = keyword.lower()
    if not keyword:
        logger.warning(f"No keyword provided for '{key}'. All PDF files will be downloaded.")

    try:
        client = initialize_client(url, credentials, key)
    except Exception:
        return

    download_dir = os.path.join(output_path, key)
    os.makedirs(download_dir, exist_ok=True)
    logger.info(f"Download directory for '{key}': {download_dir}")

    try:
        files = list_files(client, key)
    except Exception:
        return

    for file in files:
        logger.debug(f"Found file: {file}")

    for file in files:
        file_lower = file.lower()
        is_pdf = file_lower.endswith(".pdf")
        contains_keyword = keyword in file_lower if keyword else False

        if is_pdf and (contains_keyword or not keyword):
            logger.info(f"Attempting to download file: {file}")
            local_filename = os.path.basename(file)
            local_path = os.path.join(download_dir, local_filename)
            try:
                download_file(client, file, local_path, dry_run, key)
            except Exception:
                continue
        else:
            if is_pdf:
                logger.info(f"Skipped PDF file (keyword not found): {file}")
            else:
                logger.info(f"Skipped non-PDF file: {file}")


def downloader(urls: Dict[str, str], credentials: Dict[str, str], keywords: Dict[str, str], dry_run: bool, output_path: str = "./downloads/") -> None:
    """
    Sync timetables from multiple WebDAV URLs, downloading only PDF files that contain specified keywords.

    Args:
        urls (Dict[str, str]): A dictionary where keys are identifiers and values are WebDAV URLs.
        credentials (Dict[str, str]): A dictionary containing 'username' and 'password'.
        keywords (Dict[str, str]): A dictionary mapping each URL identifier to a keyword.
        dry_run (bool): If True, simulate actions without making actual changes.
        output_path (str, optional): Base directory to download files into. Defaults to "./downloads/".
    """
    os.makedirs(output_path, exist_ok=True)
    logger.info(f"Base download directory set to: {output_path}")

    for key, url in urls.items():
        keyword = keywords.get(key, "")
        process_url(key, url, credentials, keyword, dry_run, output_path)

    logger.info("Finished downloading timetables.")


def main():
    """
    Main execution function.
    """
    try:
        # Load the configuration
        config = load_config("config/config.yaml")
        urls = config.get('urls')
        credentials = config.get('credentials')
        keywords = config.get('keywords', {})  # Default to empty dict if not provided
        dry_run = config.get('dry_run', False)

        if not urls or not credentials:
            logger.error("Configuration must include 'urls' and 'credentials'.")
            exit(1)

        if not keywords:
            logger.warning("No keywords provided. All PDF files from all URLs will be downloaded.")

        downloader(urls, credentials, keywords, dry_run)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
