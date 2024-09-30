import logging
from pathlib import Path
from timetable_scraper.libs.helper_functions import load_config, read_csv, save_to_csv
from timetable_scraper.libs.openai_parser import openai_parser
from timetable_scraper.libs.get_timetable_ver import extract_version, log_pdf_versions
from timetable_scraper.libs.camelot_pdf_to_df import extract_tables, save_raw_tables, convert_tablelist_to_dataframe
from timetable_scraper.libs.sync_timetables import sync_timetables
from timetable_scraper.libs.process_raw_data import process_data
from timetable_scraper.libs.google_calendar_api import GoogleCalendarAPI
from timetable_scraper.libs.log_config import setup_logger

# Set up the logger
setup_logger()
logger = logging.getLogger(__name__)

def main():
    # Load configuration
    config = load_config()
    logger.info("Loaded configuration.")

    # Synchronize and download timetables
    sync_timetables(config["WebDAV"]["urls"], config["WebDAV"]["credentials"], config["WebDAV"]["keywords"], config["dry_run"])
    
    # Log PDF versions
    log_pdf_versions(config["paths"]["downloads"], config["paths"]["csv_log"])

    # Process PDF files
    download_path = config["paths"]["downloads"]
    output_dir = config["paths"]["output"]
    
    pdf_files = list(Path(download_path).glob("*.pdf"))
    for pdf_file in pdf_files:
        # Extract tables
        tables = extract_tables(str(pdf_file))
        if tables:
            # Save raw tables
            save_raw_tables(tables, output_dir)
            
            # Convert to dataframe
            df = convert_tablelist_to_dataframe(tables)
            
            # Process dataframe
            processed_df = process_data(df, config["OpenAI"]["api_key"])
            
            # Save processed data
            save_to_csv(processed_df, Path(output_dir) / f"processed_{pdf_file.stem}.csv")
    
    # Update Google Calendar if enabled
    if config["GoogleCalendar"]["enabled"]:
        calendar_api = GoogleCalendarAPI(config["GoogleCalendar"]["calendar_id"], config["GoogleCalendar"]["time_zone"])
        # Example: Add further interaction with Google Calendar API here
    
    logger.info("Timetable scraping completed.")

if __name__ == "__main__":
    main()