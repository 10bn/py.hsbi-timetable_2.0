import logging
import os
import shutil

from libs.downloader import WebDAVDownloader
from libs.timetable_version import extract_version_from_pdf
from libs.utils import load_config
from libs.logger import setup_logger

# ================================
# Logger Setup
# ================================

setup_logger()
logger = logging.getLogger(__name__)


def ensure_directory_exists(directory):
    """
    Ensures that the specified directory exists. Creates it if it doesn't exist.
    Args:
        directory (str): The directory path.
    """
    if not os.path.exists(directory):
        logger.info(f"Directory '{directory}' does not exist. Creating it.")
        os.makedirs(directory)
    else:
        logger.info(f"Directory '{directory}' already exists.")


def get_existing_versions(download_dir):
    """
    Scan the base download directory and retrieve the existing timetable versions.

    Args:
        download_dir (str): Path to the download directory.

    Returns:
        dict: Dictionary of timetable keys and their corresponding versions.
    """
    ensure_directory_exists(download_dir)

    logger.info(f"Scanning for existing versions in '{download_dir}'")
    timetable_versions = {}
    
    for folder in filter(lambda f: f != "temp" and os.path.isdir(os.path.join(download_dir, f)), os.listdir(download_dir)):
        folder_path = os.path.join(download_dir, folder)
        versions = [v for v in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, v))]
        logger.info(f"Found {len(versions)} versions for timetable '{folder}': {versions}")
        timetable_versions[folder] = versions

    logger.info("Completed scanning existing versions.")
    return timetable_versions


def process_downloaded_files(download_path, timetable_key, downloader, existing_versions):
    """
    Process the downloaded files for a specific timetable, extract versions, 
    and move them to the appropriate directory if they are new.

    Args:
        download_path (str): Path to the downloaded files.
        timetable_key (str): The timetable identifier.
        downloader (WebDAVDownloader): Instance of the downloader.
        existing_versions (dict): Dictionary of existing versions.
    """
    logger.info(f"Processing downloaded files for timetable '{timetable_key}' in '{download_path}'")
    downloaded_files = [f for f in os.listdir(download_path) if f.lower().endswith(".pdf")]

    if not downloaded_files:
        logger.warning(f"No PDF files found for timetable '{timetable_key}' in '{download_path}'")
        return

    for file in downloaded_files:
        full_file_path = os.path.join(download_path, file)
        logger.info(f"Extracting version from '{full_file_path}'")
        version = str(extract_version_from_pdf(full_file_path, return_timestamp=False))

        logger.info(f"Extracted version '{version}' for timetable '{timetable_key}'")
        if version not in existing_versions.get(timetable_key, []):
            logger.info(f"New version detected for '{timetable_key}': {version}")
            target_dir = os.path.join(downloader.base_download_dir, timetable_key, version)
            os.makedirs(target_dir, exist_ok=True)
            logger.info(f"Moving file '{file}' to '{target_dir}'")
            shutil.move(full_file_path, os.path.join(target_dir, file))


def download_and_compare_timetables(existing_versions, downloader, timetables):
    """
    Download the timetables from WebDAV, extract and compare their versions, 
    and move new versions to their respective folders.

    Args:
        existing_versions (dict): Dictionary of existing versions.
        downloader (WebDAVDownloader): Instance of the downloader.
        timetables (dict): Timetable configuration from the config file.
    """
    temp_download_dir = os.path.join(downloader.base_download_dir, "temp")
    os.makedirs(temp_download_dir, exist_ok=True)
    logger.info(f"Created temporary directory '{temp_download_dir}' for downloading timetables.")

    # Add timetables to the downloader
    for timetable_key, timetable in timetables.items():
        download_path = os.path.join(temp_download_dir, timetable_key)
        logger.info(f"Adding timetable '{timetable_key}' with keywords {timetable['keywords']} to downloader")
        downloader.add_timetable(timetable["keywords"], download_path)

    logger.info("Starting the WebDAV download process.")
    downloader.run()

    # Process each timetable's downloaded files
    logger.info("Processing downloaded timetables for version comparison.")
    for timetable_key in timetables.keys():
        download_path = os.path.join(temp_download_dir, timetable_key)
        process_downloaded_files(download_path, timetable_key, downloader, existing_versions)

    # Clean up the temporary directory
    logger.info(f"Cleaning up temporary directory '{temp_download_dir}'.")
    shutil.rmtree(temp_download_dir)
    logger.info(f"Temporary directory '{temp_download_dir}' removed.")


def main():
    """
    Main function to set up downloader, fetch existing timetable versions, 
    and download and compare timetables.
    """
    logger.info("Application started. Loading configuration.")
    config = load_config()

    downloader = WebDAVDownloader(
        url=config["webdav"]["url"],
        username=config["webdav"]["username"],
        password=config["webdav"]["password"],
        dry_run=config["general"]["dry_run"],
        base_download_dir=config["path_settings"]["download_dir"],
    )
    logger.info("WebDAVDownloader initialized successfully.")

    # Ensure the base download directory exists
    ensure_directory_exists(downloader.base_download_dir)

    # Retrieve existing timetable versions
    existing_versions = get_existing_versions(downloader.base_download_dir)

    if not existing_versions:
        logger.warning("No existing versions found. Consider downloading at least one version manually.")

    # Download and compare timetables
    download_and_compare_timetables(existing_versions, downloader, config["timetables"])

    logger.info("Application finished successfully.")


if __name__ == "__main__":
    main()
