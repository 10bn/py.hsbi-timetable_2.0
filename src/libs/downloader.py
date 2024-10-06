# src/libs/downloader.py

import logging
from pathlib import Path
from typing import Dict, List
from webdav3.client import Client

logger = logging.getLogger(__name__)

# ================================
# WebDAV Downloader Class
# ================================


class WebDAVDownloader:
    """
    A class to handle downloading files from a WebDAV server based on specified keywords.
    """

    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        dry_run: bool = False,
        base_download_dir: str = "./downloads/",
    ) -> None:
        """
        Initialize the WebDAVDownloader.

        Args:
            url (str): WebDAV server URL.
            username (str): WebDAV username.
            password (str): WebDAV password.
            dry_run (bool, optional): If True, simulate actions without performing downloads. Defaults to False.
            base_download_dir (str, optional): Base directory to download files into. Defaults to "./downloads/".
        """
        self.url = url
        self.username = username
        self.password = password
        self.dry_run = dry_run
        self.base_download_dir = Path(base_download_dir)
        self.timetables: List[Dict[str, List[str]]] = []

        # Initialize WebDAV client
        self.client = self.initialize_client()

    def initialize_client(self) -> Client:
        """
        Initialize and return a WebDAV client.

        Returns:
            Client: Configured WebDAV client.
        """
        options = {
            "webdav_hostname": self.url,
            "webdav_login": self.username,
            "webdav_password": self.password,
            "webdav_port": 443,  # Default HTTPS port
            "webdav_root": "/",
            "webdav_timeout": 30,
            "webdav_chunk_size": 32768,
            "webdav_ssl_verify": True,
        }

        try:
            client = Client(options)
            # Assuming 'verify' is not a valid attribute for webdav3.Client
            # If it is required, ensure it's correctly set
            logger.debug("WebDAV client initialized successfully.")
            return client
        except Exception as e:
            logger.error(f"Failed to initialize WebDAV client: {e}")
            raise

    def add_timetable(self, keywords: List[str], download_path: str) -> None:
        """
        Add a timetable with its list of keywords and download path.

        Args:
            keywords (List[str]): List of keywords to filter files. All keywords must be present in the filename.
            download_path (str): Local path to save downloaded files.
        """
        if not keywords or not download_path:
            logger.error("Both keywords and download_path must be provided.")
            return

        keywords_lower = [keyword.lower() for keyword in keywords]
        download_path = Path(download_path)
        download_path.mkdir(parents=True, exist_ok=True)

        logger.debug(
            f"Added timetable with keywords {keywords_lower} and download path '{download_path}'."
        )
        self.timetables.append(
            {"keywords": keywords_lower, "download_path": download_path}
        )

    def list_files(self) -> List[str]:
        """
        List all files in the WebDAV server.

        Returns:
            List[str]: List of file paths.
        """
        try:
            files = self.client.list()
            logger.info(
                f"Retrieved {len(files)} files from the WebDAV server."
            )
            return files
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise

    def download_file(self, remote_path: str, local_path: Path) -> None:
        """
        Download a single file from the WebDAV server.

        Args:
            remote_path (str): Path to the remote file.
            local_path (Path): Path where the file will be saved locally.
        """
        if self.dry_run:
            logger.info(
                f"Dry run enabled. Skipping download of '{remote_path}' to '{local_path}'."
            )
            return

        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.client.download_sync(
                remote_path=remote_path, local_path=str(local_path)
            )
            logger.info(f"Downloaded '{remote_path}' to '{local_path}'.")
        except Exception as e:
            logger.error(f"Failed to download '{remote_path}': {e}")

    def run(self) -> None:
        """
        Execute the download process for all added timetables.
        """
        logger.info("Starting the WebDAV download process.")

        try:
            all_files = self.list_files()
        except Exception:
            logger.error(
                "Aborting download process due to failure in listing files."
            )
            return

        for timetable in self.timetables:
            keywords = timetable["keywords"]
            download_path = timetable["download_path"]
            logger.info(f"Processing timetable with keywords {keywords}.")

            matching_files = [
                file
                for file in all_files
                if all(keyword in file.lower() for keyword in keywords)
            ]

            if not matching_files:
                logger.warning(
                    f"No files found containing all keywords {keywords}."
                )
                continue

            logger.info(
                f"Found {len(matching_files)} file(s) matching the keywords {keywords}."
            )

            for file in matching_files:
                if not file.lower().endswith(".pdf"):
                    logger.info(f"Skipped non-PDF file: {file}")
                    continue

                local_filename = Path(file).name
                local_file_path = download_path / local_filename

                logger.debug(
                    f"Preparing to download '{file}' to '{local_file_path}'."
                )
                self.download_file(file, local_file_path)

        logger.info("Completed the WebDAV download process.")
