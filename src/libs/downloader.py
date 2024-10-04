# src/libs/webdav_downloader.py

import logging
import os
from typing import Dict, List

from webdav3.client import Client

from utils import load_config
from logger import setup_logger

# Setup logging
setup_logger()
logger = logging.getLogger(__name__)


# ================================
# WebDAV Downloader Class
# ================================

class WebDAVDownloader:
    """
    A class to handle downloading files from multiple WebDAV servers based on specified keywords.
    """

    def __init__(
        self,
        urls: Dict[str, str],
        credentials: Dict[str, str],
        keywords: Dict[str, str],
        dry_run: bool = False,
        output_path: str = "./downloads/"
    ):
        """
        Initialize the WebDAVDownloader.

        Args:
            urls (Dict[str, str]): A dictionary where keys are identifiers and values are WebDAV URLs.
            credentials (Dict[str, str]): A dictionary containing 'username' and 'password'.
            keywords (Dict[str, str]): A dictionary mapping each URL identifier to a keyword.
            dry_run (bool, optional): If True, simulate actions without performing downloads. Defaults to False.
            output_path (str, optional): Base directory to download files into. Defaults to "./downloads/".
        """
        self.urls = urls
        self.credentials = credentials
        self.keywords = keywords
        self.dry_run = dry_run
        self.output_path = output_path

        # Ensure the base output directory exists
        os.makedirs(self.output_path, exist_ok=True)
        logger.debug(f"Initialized WebDAVDownloader with output directory: {self.output_path}")

    def initialize_client(self, url: str, key: str) -> Client:
        """
        Initialize a WebDAV client.

        Args:
            url (str): WebDAV URL.
            key (str): Identifier for logging purposes.

        Returns:
            Client: Configured WebDAV client.

        Raises:
            Exception: If client initialization fails.
        """
        options = {
            "webdav_hostname": url,
            "webdav_login": self.credentials.get("username"),
            "webdav_password": self.credentials.get("password"),
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

    def list_files(self, client: Client, key: str) -> List[str]:
        """
        Retrieve a list of files from the WebDAV server.

        Args:
            client (Client): Configured WebDAV client.
            key (str): Identifier for logging purposes.

        Returns:
            List[str]: List of file paths.

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

    def download_file(self, client: Client, remote_path: str, local_path: str, key: str) -> None:
        """
        Download a single file from the WebDAV server.

        Args:
            client (Client): Configured WebDAV client.
            remote_path (str): Path to the remote file.
            local_path (str): Path where the file will be saved locally.
            key (str): Identifier for logging purposes.

        Raises:
            Exception: If downloading the file fails.
        """
        if self.dry_run:
            logger.info(f"Dry run enabled. Skipping download of '{remote_path}' to '{local_path}'.")
            return

        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            client.download_sync(remote_path=remote_path, local_path=local_path)
            logger.info(f"Successfully downloaded '{remote_path}' to '{local_path}'")
        except Exception as e:
            logger.error(f"Failed to download file '{remote_path}' from '{key}': {e}")
            raise

    def process_url(self, key: str, url: str, keyword: str) -> None:
        """
        Process a single WebDAV URL: list files and download relevant PDFs based on the keyword.

        Args:
            key (str): Identifier for the URL.
            url (str): WebDAV URL.
            keyword (str): Keyword to filter PDF files.
        """
        logger.info(f"Processing URL for '{key}': {url}")

        if not isinstance(keyword, str):
            logger.error(f"Keyword for '{key}' must be a string.")
            return

        keyword = keyword.lower()
        if not keyword:
            logger.warning(f"No keyword provided for '{key}'. All PDF files will be downloaded.")

        try:
            client = self.initialize_client(url, key)
        except Exception:
            return

        download_dir = os.path.join(self.output_path, key)
        os.makedirs(download_dir, exist_ok=True)
        logger.info(f"Download directory for '{key}': {download_dir}")

        try:
            files = self.list_files(client, key)
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
                    self.download_file(client, file, local_path, key)
                except Exception:
                    continue
            else:
                if is_pdf:
                    logger.info(f"Skipped PDF file (keyword not found): {file}")
                else:
                    logger.info(f"Skipped non-PDF file: {file}")

    def run(self) -> None:
        """
        Execute the download process for all configured URLs.
        """
        logger.info("Starting the WebDAV download process.")

        for key, url in self.urls.items():
            keyword = self.keywords.get(key, "")
            self.process_url(key, url, keyword)

        logger.info("Finished downloading timetables.")


# ================================
# Main Execution Function
# ================================

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
        output_path = config.get('output_path', "./downloads/")

        if not urls or not credentials:
            logger.error("Configuration must include 'urls' and 'credentials'.")
            exit(1)

        if not keywords:
            logger.warning("No keywords provided. All PDF files from all URLs will be downloaded.")

        downloader = WebDAVDownloader(
            urls=urls,
            credentials=credentials,
            keywords=keywords,
            dry_run=dry_run,
            output_path=output_path
        )
        downloader.run()

    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
