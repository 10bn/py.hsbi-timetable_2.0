# src/main.py

import logging
from pathlib import Path
import shutil
import json

from libs.downloader import WebDAVDownloader
from libs.timetable_version import extract_version_from_pdf
from libs.utils import load_config
from libs.logger import setup_logger
from libs.parser import PdfParser  
from libs.update_google_calendar import GoogleCalendarAPI, create_all_events, delete_all_events, save_events_to_csv

# ================================
# Load Configuration
# ================================

# Load the configuration first to access log_level
config = load_config()

# Extract log_level from config and convert it to a logging level
log_level_str = config.get("general", {}).get("log_level", "WARNING").upper()
log_level = getattr(logging, log_level_str, logging.WARNING)

# ================================
# Logger Setup
# ================================

# Setup logger with the configured log level
setup_logger(log_level)

# Obtain a logger for this module
logger = logging.getLogger(__name__)

# ================================
# Function Definitions
# ================================

def get_existing_versions(download_dir: Path) -> dict:
    """
    Scan the base download directory and retrieve the existing timetable versions.

    Args:
        download_dir (Path): Path to the download directory.

    Returns:
        dict: Dictionary of timetable keys and their corresponding versions.
    """
    # Inline directory existence check and creation
    download_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Directory '{download_dir}' ensured.")

    logger.info(f"Scanning for existing versions in '{download_dir}'")
    timetable_versions = {}

    for folder in filter(lambda f: f.name != "temp" and f.is_dir(), download_dir.iterdir()):
        versions = [v.name for v in folder.iterdir() if v.is_dir()]
        logger.info(f"Found {len(versions)} versions for timetable '{folder.name}': {versions}")
        timetable_versions[folder.name] = versions

    logger.info("Completed scanning existing versions.")
    return timetable_versions


def process_downloaded_files(
    download_path: Path, timetable_key: str, downloader: WebDAVDownloader, existing_versions: dict
):
    """
    Process the downloaded files for a specific timetable, extract versions,
    and move them to the appropriate directory if they are new.

    Args:
        download_path (Path): Path to the downloaded files.
        timetable_key (str): The timetable identifier.
        downloader (WebDAVDownloader): Instance of the downloader.
        existing_versions (dict): Dictionary of existing versions.
    """

    logger.info(f"Processing downloaded files for timetable '{timetable_key}' in '{download_path}'")
    downloaded_files = list(download_path.glob("*.pdf"))

    if not downloaded_files:
        logger.warning(f"No PDF files found for timetable '{timetable_key}' in '{download_path}'")
        return

    for file in downloaded_files:
        version = extract_version_from_pdf(str(file))
        if version is None:
            logger.warning(f"Could not extract version from '{file.name}'. Skipping this file.")
            continue

        # Check if the version already exists
        existing_versions_list = existing_versions.get(timetable_key, [])
        if version not in existing_versions_list:
            logger.info(f"New version detected for '{timetable_key}': {version}")
            target_dir = downloader.base_download_dir / timetable_key / version
            # Inline directory existence check and creation
            target_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Directory '{target_dir}' ensured.")
            logger.info(f"Moving file '{file.name}' to '{target_dir}'")
            shutil.move(str(file), target_dir / file.name)
            # Update existing_versions to include the new version
            existing_versions.setdefault(timetable_key, []).append(version)
        else:
            logger.info(f"Version '{version}' for '{timetable_key}' already exists. Skipping.")


def download_and_compare_timetables(existing_versions: dict, downloader: WebDAVDownloader, timetables: dict):
    """
    Download the timetables from WebDAV, extract and compare their versions,
    and move new versions to their respective folders.

    Args:
        existing_versions (dict): Dictionary of existing versions.
        downloader (WebDAVDownloader): Instance of the downloader.
        timetables (dict): Timetable configuration from the config file.
    """
    temp_download_dir = downloader.base_download_dir / "temp"
    # Inline directory existence check and creation
    temp_download_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Directory '{temp_download_dir}' ensured.")
    logger.info(f"Created temporary directory '{temp_download_dir}' for downloading timetables.")

    # Add timetables to the downloader
    for timetable_key, timetable in timetables.items():
        download_path = temp_download_dir / timetable_key
        # Inline directory existence check and creation
        download_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Directory '{download_path}' ensured.")
        logger.info(f"Adding timetable '{timetable_key}' with keywords {timetable['keywords']} to downloader")
        downloader.add_timetable(timetable["keywords"], str(download_path))

    logger.info("Starting the WebDAV download process.")
    downloader.run()

    # Process each timetable's downloaded files
    logger.info("Processing downloaded timetables for version comparison.")
    for timetable_key in timetables.keys():
        process_downloaded_files(temp_download_dir / timetable_key, timetable_key, downloader, existing_versions)

    # Clean up the temporary directory
    logger.info(f"Cleaning up temporary directory '{temp_download_dir}'.")
    shutil.rmtree(temp_download_dir)
    logger.info(f"Temporary directory '{temp_download_dir}' removed.")


def parse_and_save_pdf(api_key: str, pdf_path: str, output_dir: str = "output", save_raw: bool = False, save_csv_events: bool = False, save_json_events: bool = False):
    """
    Parse the given PDF and save the extracted events.

    Args:
        api_key (str): OpenAI API key.
        pdf_path (str): Path to the PDF file.
        output_dir (str, optional): Directory to save outputs. Defaults to "output".
        save_raw (bool, optional): Whether to save raw tables. Defaults to False.
        save_csv_events (bool, optional): Whether to save the events DataFrame as CSV. Defaults to False.
    """
    logger.info(f"Starting PDF parsing for: {pdf_path}")
    parser = PdfParser(api_key=api_key, output_dir=output_dir)
    df = parser.parse_pdf(pdf_path, save_raw=save_raw, save_csv_events=save_csv_events, save_json_events=True)

    if df is not None:
        logger.info("Parsed DataFrame:")
        logger.info(df.head())
    else:
        logger.error("Parsing failed.")


def update_google_calendar(calendar_config: dict):
    """
    Update Google Calendar with the parsed events.

    Args:
        calendar_config (dict): Configuration for Google Calendar API.
    """
    logger.info("Starting Google Calendar update process.")
    calendar_api = GoogleCalendarAPI(
        calendar_id=calendar_config["calendar_id"],
        time_zone=calendar_config["time_zone"],
        scopes=calendar_config["scopes"],
        token_json_file=calendar_config["token_json_file"],
        credentials_json_file=calendar_config["credentials_json_file"],
        max_results=calendar_config.get("max_results", 2500),
        dry_run=calendar_config.get("dry_run", False),
    )

    # Load local events from JSON file
    try:
        with open("/workspaces/py.hsbi-timetable_2.0/output/Stundenplan WS_2024_2025_ELM 3_Stand 2024-10-11_events.json", "r") as file:
            local_events = json.load(file)
            logger.info(f"Found {len(local_events)} events in the timetable.")
    except json.JSONDecodeError as e:
        logger.error(f"Error reading final_events.json: {e}")
        return

    if calendar_config.get("dry_run", False):
        created_events = create_all_events(calendar_api, local_events)
        save_events_to_csv(created_events, "output/dry_run_output.csv")
    else:
        delete_all_events(calendar_api, calendar_config["time_zone"])
        create_all_events(calendar_api, local_events)

    logger.info("Google Calendar update process completed.")


def main_flow():
    """
    Orchestrate the downloading, parsing, and calendar updating processes.
    """
    try:
        logger.info("Starting the main application flow.")

        # Initialize WebDAVDownloader with configuration
        downloader = WebDAVDownloader(
            url=config["webdav"]["url"],
            username=config["webdav"]["username"],
            password=config["webdav"]["password"],
            dry_run=config["general"]["dry_run"],
            base_download_dir=Path(config["path_settings"]["download_dir"]),
        )
        logger.info("WebDAVDownloader initialized successfully.")

        # Ensure the base download directory exists
        downloader.base_download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Directory '{downloader.base_download_dir}' ensured.")

        # Retrieve existing timetable versions
        existing_versions = get_existing_versions(downloader.base_download_dir)

        if not existing_versions:
            logger.warning("No existing versions found. Consider downloading at least one version manually.")

        # Download and compare timetables
        download_and_compare_timetables(existing_versions, downloader, config["timetables"])

        # Parse PDFs and update Google Calendar
        for timetable_key in config["timetables"].keys():
            latest_version_dir = downloader.base_download_dir / timetable_key / sorted(existing_versions[timetable_key])[-1]
            # Assuming the latest PDF is the one to parse
            pdf_files = list(latest_version_dir.glob("*.pdf"))
            if pdf_files:
                parse_and_save_pdf(
                    api_key=config["openai"]["api_key"],
                    pdf_path=str(pdf_files[0]),
                    output_dir="output",
                    save_raw=True,
                    save_csv_events=True,
                    save_json_events=True
                )
            else:
                logger.warning(f"No PDF files found in '{latest_version_dir}' for parsing.")

        # Update Google Calendar
        update_google_calendar(config["google_calendar"])

        logger.info("Main application flow completed successfully.")

    except Exception as e:
        logger.critical(f"An unhandled exception occurred: {e}", exc_info=True)


if __name__ == "__main__":
    main_flow()
