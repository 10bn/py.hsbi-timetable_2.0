import re
from datetime import datetime
from pathlib import Path
from typing import Optional  # Ensure this line is present
import logging
import fitz

logger = logging.getLogger(__name__)


def extract_version_from_pdf(pdf_path: str) -> Optional[str]:
    """
    Extracts the version information from the first page of a PDF file.
    The version information is expected to be in the format "Version: DD.MM.YYYY, HH:MM Uhr".
    The extracted version is returned as a formatted datetime string "YYYY-MM-DD HH:MM:SS".
    Args:
        pdf_path (str): The file path to the PDF document.
    Returns:
        Optional[str]: The extracted version as a formatted datetime string, or None if the version
                       information is not found or an error occurs.
    Raises:
        None: All exceptions are caught and logged, and None is returned in case of an error.
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

            first_page_text = pdf_document.load_page(0).get_text()

            # Define the regex pattern to match the version string
            version_pattern = (
                r"Version:\s*(\d{2}\.\d{2}\.\d{4}),\s*(\d{2}:\d{2})\s*Uhr"
            )
            match = re.search(version_pattern, first_page_text)

            if match:
                date_version, time_version = (
                    match.groups()
                )  # Unpacking directly from match groups

                # Parse and format the combined date and time string
                version_datetime = datetime.strptime(
                    f"{date_version} {time_version}", "%d.%m.%Y %H:%M"
                )
                formatted_datetime = version_datetime.strftime(
                    "%Y-%m-%d_%H-%M-%S"
                )

                logger.info(
                    f"Extracted version from '{pdf_file.name}': {formatted_datetime}"
                )
                return formatted_datetime

            logger.warning(
                f"Version pattern not found in the PDF: {pdf_file.name}"
            )
            return None

    except Exception as e:
        logger.error(
            f"An error occurred while extracting version from '{pdf_file.name}': {e}"
        )
        return None
