import logging
import os
from libs.downloader import WebDAVDownloader
from libs.parser import PdfParser
from libs.update_google_calendar import GoogleCalendarAPI
from libs.utils import load_config, save_events_to_json
from libs.logger import setup_logger

# Setup logger
setup_logger()
logger = logging.getLogger(__name__)


def main():
    """
    Main function to execute the workflow of downloading, parsing PDFs, and updating Google Calendar.
    """
    # Step 1: Load configuration
    config = load_config("config/config.yaml")

    # Extract required values from the configuration
    api_key = config["openai"]["api_key"]
    timetables = config["webdav"]["timetables"]
    credentials = config["webdav"]["credentials"]
    token_json_file = config["google_calendar"]["auth"]["token_json_file"]
    credentials_json_file = config["google_calendar"]["auth"]["credentials_json_file"]
    time_zone = config["google_calendar"]["time_zone"]
    download_dir = config["output"]["download_dir"]
    output_dir = config["output"]["output_dir"]
    dry_run = config["general"]["dry_run"]

    for timetable in timetables:
        # Step 2: Download PDF files
        print(timetable)


 

if __name__ == "__main__":
    main()
