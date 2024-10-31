import logging
from webdav3.client import Client
import os
from helper_functions import load_config
from log_config import setup_logger

# Set up the logger
setup_logger()
logger = logging.getLogger(__name__)

def sync_timetables(urls: dict, credentials: dict, keywords: dict, dry_run: bool, output_path: str = "./downloads/") -> None:
    """
    Sync timetables from multiple WebDAV URLs, downloading only PDF files that contain specified keywords.

    Args:
        urls (dict): A dictionary where keys are identifiers and values are WebDAV URLs.
        credentials (dict): A dictionary containing 'username' and 'password'.
        keywords (dict): A dictionary mapping each URL identifier to a keyword.
        dry_run (bool): If True, simulate actions without making actual changes.
        output_path (str, optional): Base directory to download files into. Defaults to "./downloads/".
    """
    # Create base output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    logger.info(f"Base download directory set to: {output_path}")

    for key, url in urls.items():
        logger.info(f"Processing URL for '{key}': {url}")

        # Retrieve keyword for the current URL
        url_keyword = keywords.get(key, "")
        if not isinstance(url_keyword, str):
            logger.error(f"Keyword for '{key}' must be a string.")
            continue

        # Convert keyword to lowercase for case-insensitive matching
        url_keyword = url_keyword.lower()

        if not url_keyword:
            logger.warning(f"No keyword provided for '{key}'. All PDF files will be downloaded.")

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
        except Exception as e:
            logger.error(f"Failed to initialize WebDAV client for '{key}': {e}")
            continue

        # Create a directory for the current URL if it doesn't exist
        download_dir = os.path.join(output_path, key)
        os.makedirs(download_dir, exist_ok=True)
        logger.info(f"Download directory for '{key}': {download_dir}")

        try:
            files = client.list()
            logger.info(f"Retrieved {len(files)} files from '{key}'")
        except Exception as e:
            logger.error(f"Failed to list files for '{key}': {e}")
            continue

        for file in files:
            logger.debug(f"Found file: {file}")

        # Download all PDF files that contain the keyword in the filename
        for file in files:
            file_lower = file.lower()
            is_pdf = file_lower.endswith(".pdf")
            contains_keyword = url_keyword in file_lower if url_keyword else False

            if is_pdf and (contains_keyword or not url_keyword):
                logger.info(f"Attempting to download file: {file}")
                if dry_run:
                    logger.info(f"Dry run enabled. Skipping download of '{file}' to '{os.path.join(download_dir, os.path.basename(file))}'.")
                    continue
                try:
                    remote_path = file
                    local_filename = os.path.basename(file)
                    local_path = os.path.join(download_dir, local_filename)
                    # Ensure the local directory exists
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    client.download_sync(remote_path=remote_path, local_path=local_path)
                    logger.info(f"Successfully downloaded '{file}' to '{local_path}'")
                except Exception as e:
                    logger.error(f"Failed to download file '{file}' from '{key}': {e}")
            else:
                if is_pdf:
                    logger.info(f"Skipped PDF file (keyword not found): {file}")
                else:
                    logger.info(f"Skipped non-PDF file: {file}")

    logger.info("Finished downloading timetables.")

if __name__ == "__main__":
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

        sync_timetables(urls, credentials, keywords, dry_run)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")