# src/libs/timetable_version.py

import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Union
import logging
import fitz 
from logger import setup_logger


setup_logger()
logger = logging.getLogger(__name__)


def extract_version(pdf_path: str, return_timestamp: bool = True) -> Optional[Union[float, str]]:
    """
    Extract the version date and time from the first page of a PDF.

    This function searches for a specific pattern:
    "Version: DD.MM.YYYY, HH:MM Uhr" and returns the extracted version as either
    a Unix timestamp or a human-readable string.

    Args:
        pdf_path (str): The file path to the PDF.
        return_timestamp (bool, optional): If True, return the version as a Unix timestamp.
                                           If False, return it as a human-readable string.

    Returns:
        Optional[Union[float, str]]: The extracted version as a timestamp (float) if return_timestamp is True,
                                     or a human-readable string if return_timestamp is False.
                                     Returns None if the version cannot be extracted.
    """
    pdf_file = Path(pdf_path)

    if not pdf_file.is_file():
        logger.error(f"The specified PDF file does not exist: {pdf_path}")
        return None

    try:
        logger.info(f"Opening PDF file: {pdf_path}")
        with fitz.open(pdf_file) as pdf_document:
            if pdf_document.page_count < 1:
                logger.warning(f"The PDF file has no pages: {pdf_path}")
                return None

            first_page = pdf_document.load_page(0)  # Load the first page (0-indexed)
            first_page_text = first_page.get_text()

            # Define the regex pattern to match the version string
            version_pattern = r"Version:\s*(\d{2}\.\d{2}\.\d{4}),\s*(\d{2}:\d{2})\s*Uhr"
            match = re.search(version_pattern, first_page_text)

            if match:
                date_version = match.group(1)  # Example: "26.09.2024"
                time_version = match.group(2)  # Example: "11:13"

                # Combine date and time into a single string
                version_str = f"{date_version} {time_version}"

                # Parse the combined string into a datetime object
                version_datetime = datetime.strptime(version_str, "%d.%m.%Y %H:%M")
                logger.info(f"Extracted version from '{pdf_file.name}': {version_datetime}")

                if return_timestamp:
                    # Return as Unix timestamp
                    return version_datetime.timestamp()
                else:
                    # Return as human-readable string
                    return version_datetime.strftime("%Y-%m-%d %H:%M:%S")
            else:
                logger.warning(f"Version pattern not found in the PDF: {pdf_file.name}")
                return None

    except Exception as e:
        logger.error(f"An error occurred while extracting version from '{pdf_file.name}': {e}")
        return None


if __name__ == "__main__":
    # Example usage of the extract_version function
    pdf_path = "downloads/timetable_1/Stundenplan WS_2024_2025_ELM 3.pdf"
    
    # Set to True for timestamp, False for human-readable format
    return_as_timestamp = False

    version = extract_version(pdf_path, return_timestamp=return_as_timestamp)

    if version:
        if return_as_timestamp:
            print(f"Extracted Version Timestamp: {version}")
        else:
            print(f"Extracted Version (Human-readable): {version}")
    else:
        print("Failed to extract version from the PDF.")
