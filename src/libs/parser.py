import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import camelot
import pandas as pd
import openai

from libs.timetable_version import extract_version_from_pdf
from libs.utils import save_to_csv, load_config, save_events_to_json

# ================================
# Logger Setup
# ================================

logger = logging.getLogger(__name__)

# ================================
# Environment Configuration
# ================================
if os.name == "posix" and os.uname().sysname == "Darwin":
    try:
        from libs.utils import init_ghostscript_via_brew_on_mac

        logger.info("Initializing Ghostscript via Homebrew on macOS.")
        init_ghostscript_via_brew_on_mac()
        logger.info("Ghostscript initialization completed successfully.")
    except Exception as e:
        logger.error(
            f"An error occurred while initializing Ghostscript: {e}",
            exc_info=True,
        )

# ================================
# PDF Parsing Functions
# ================================


def extract_tables(pdf_path: str) -> Optional[List[camelot.core.Table]]:
    """
    Extract tables from a PDF using Camelot.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        Optional[List[camelot.core.Table]]: List of extracted tables or None if extraction fails.
    """
    try:
        logger.info(f"Starting table extraction from PDF: {pdf_path}")
        tables = camelot.read_pdf(pdf_path, flavor="lattice", pages="all")
        table_count = len(tables)
        if table_count == 0:
            logger.warning(f"No tables found in PDF: {pdf_path}")
            return None
        logger.info(f"Successfully extracted {table_count} tables from PDF.")
        return tables
    except Exception as e:
        logger.error(
            f"Failed to extract tables from PDF '{pdf_path}': {e}",
            exc_info=True,
        )
        return None


def save_raw_tables(
    table_list: List[camelot.core.Table], output_dir: str
) -> None:
    """
    Save raw tables extracted from PDF to CSV files.

    Args:
        table_list (List[camelot.core.Table]): List of tables extracted by Camelot.
        output_dir (str): Directory to save the raw CSV files.
    """
    if not table_list:
        logger.warning("No tables provided to save.")
        return

    raw_output_dir = os.path.join(output_dir, "raw_tables")
    os.makedirs(raw_output_dir, exist_ok=True)
    logger.info(
        f"Saving {len(table_list)} raw tables to directory: {raw_output_dir}"
    )

    for idx, table in enumerate(table_list, start=1):
        table_filename = os.path.join(raw_output_dir, f"raw_table_{idx}.csv")
        try:
            table.to_csv(table_filename, index=False)
            logger.debug(f"Saved raw table {idx} to '{table_filename}'.")
        except Exception as e:
            logger.error(
                f"Failed to save raw table {idx} to '{table_filename}': {e}",
                exc_info=True,
            )

    logger.info(f"All raw tables have been saved to '{raw_output_dir}'.")


def convert_tablelist_to_dataframe(
    table_list: List[camelot.core.Table],
) -> pd.DataFrame:
    """
    Convert a list of Camelot tables to a single pandas DataFrame.

    Args:
        table_list (List[camelot.core.Table]): List of tables extracted by Camelot.

    Returns:
        pd.DataFrame: Combined DataFrame from all tables.
    """
    try:
        logger.info("Converting list of tables to a single DataFrame.")
        dataframes = [
            table.df if i == 0 else table.df.iloc[1:].reset_index(drop=True)
            for i, table in enumerate(table_list)
        ]
        combined_df = pd.concat(dataframes, ignore_index=True)
        combined_df.columns = combined_df.iloc[0]
        combined_df = combined_df[1:].reset_index(drop=True)
        combined_df.drop(combined_df.columns[0], axis=1, inplace=True)
        combined_df.rename(
            columns={combined_df.columns[0]: "date"}, inplace=True
        )
        logger.debug(f"Combined DataFrame shape: {combined_df.shape}")
        return combined_df
    except Exception as e:
        logger.error(
            f"Error converting tables to DataFrame: {e}", exc_info=True
        )
        return pd.DataFrame()


def melt_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Melt the DataFrame to have 'date', 'time_slot', and 'raw_details' columns.

    Args:
        df (pd.DataFrame): DataFrame to melt.

    Returns:
        pd.DataFrame: Melted DataFrame.
    """
    try:
        logger.info(
            "Melting DataFrame to long format with 'date', 'time_slot', and 'raw_details'."
        )
        melted_df = df.melt(
            id_vars=["date"], var_name="time_slot", value_name="raw_details"
        )
        logger.debug(f"Melted DataFrame shape: {melted_df.shape}")
        return melted_df
    except Exception as e:
        logger.error(f"Error melting DataFrame: {e}", exc_info=True)
        return pd.DataFrame()


def forward_fill_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Forward fill missing dates in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame with potential missing dates.

    Returns:
        pd.DataFrame: DataFrame with forward-filled dates.
    """
    try:
        logger.info("Forward filling missing dates in the DataFrame.")
        df["date"] = df["date"].fillna(method="ffill")
        logger.debug("Missing dates have been forward filled.")
        return df
    except Exception as e:
        logger.error(f"Error forward filling dates: {e}", exc_info=True)
        return df


def clean_special_chars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean special characters in the entire DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to clean.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    try:
        logger.info("Cleaning special characters in the DataFrame.")
        replacements = {
            "\xa0": " ",
            "‐": "-",  # Replace hyphen-like characters with standard hyphen
        }
        for old, new in replacements.items():
            df = df.applymap(
                lambda x: x.replace(old, new) if isinstance(x, str) else x
            )
        logger.debug("Special characters have been cleaned.")
    except Exception as e:
        logger.error(f"Error cleaning special characters: {e}", exc_info=True)
    return df


def clean_time_slot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the 'time_slot' column by removing ' Uhr'.

    Args:
        df (pd.DataFrame): DataFrame containing the 'time_slot' column.

    Returns:
        pd.DataFrame: DataFrame with cleaned 'time_slot'.
    """
    if "time_slot" in df.columns:
        try:
            logger.info("Removing ' Uhr' from 'time_slot' column.")
            df["time_slot"] = df["time_slot"].str.replace(
                " Uhr", "", regex=False
            )
            logger.debug("'time_slot' column has been cleaned.")
        except Exception as e:
            logger.error(
                f"Error cleaning 'time_slot' column: {e}", exc_info=True
            )
    else:
        logger.warning("Column 'time_slot' does not exist in the DataFrame.")
    return df


def split_time_slot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split the 'time_slot' column into 'start_time' and 'end_time'.

    Args:
        df (pd.DataFrame): DataFrame containing the 'time_slot' column.

    Returns:
        pd.DataFrame: DataFrame with 'start_time' and 'end_time' columns.
    """
    if "time_slot" not in df.columns:
        logger.warning("Column 'time_slot' does not exist. Skipping split.")
        return df

    try:
        logger.info("Splitting 'time_slot' into 'start_time' and 'end_time'.")
        time_splits = df["time_slot"].str.split(" - ", n=1, expand=True)
        time_splits.columns = ["start_time_str", "end_time_str"]

        df["start_time"] = pd.to_datetime(
            time_splits["start_time_str"].str.strip(),
            format="%H.%M",
            errors="coerce",
        ).dt.time
        df["end_time"] = pd.to_datetime(
            time_splits["end_time_str"].str.strip(),
            format="%H.%M",
            errors="coerce",
        ).dt.time

        # Log any parsing issues
        start_time_issues = df["start_time"].isna()
        end_time_issues = df["end_time"].isna()

        if start_time_issues.any():
            problematic_slots = df.loc[start_time_issues, "time_slot"].tolist()
            logger.warning(
                f"Failed to parse 'start_time' for entries: {problematic_slots}"
            )

        if end_time_issues.any():
            problematic_slots = df.loc[end_time_issues, "time_slot"].tolist()
            logger.warning(
                f"Failed to parse 'end_time' for entries: {problematic_slots}"
            )

        # Drop the original 'time_slot' column
        df.drop("time_slot", axis=1, inplace=True)
        logger.debug(
            "'time_slot' column has been replaced with 'start_time' and 'end_time'."
        )
    except Exception as e:
        logger.error(f"Error splitting 'time_slot' column: {e}", exc_info=True)

    return df


def format_date(df: pd.DataFrame, current_year: int) -> pd.DataFrame:
    """
    Format the 'date' column by replacing month names and adding the year.

    Args:
        df (pd.DataFrame): DataFrame with a 'date' column.
        current_year (int): Year to append to the dates.

    Returns:
        pd.DataFrame: DataFrame with formatted 'date' column.
    """
    month_mapping = {
        "Jan": "Jan",
        "Feb": "Feb",
        "Mär": "Mar",
        "Apr": "Apr",
        "Mai": "May",
        "Jun": "Jun",
        "Jul": "Jul",
        "Aug": "Aug",
        "Sep": "Sep",
        "Okt": "Oct",
        "Nov": "Nov",
        "Dez": "Dec",
    }
    current_year_str = str(current_year)
    logger.info(f"Formatting 'date' column with year: {current_year_str}")

    try:
        df["date"] = df["date"].replace(month_mapping, regex=True)
        df["date"] = pd.to_datetime(
            df["date"].astype(str) + f" {current_year_str}",
            format="%d. %b %Y",
            errors="coerce",
        )

        if df["date"].isna().any():
            failed_dates = df.loc[df["date"].isna(), "date"].tolist()
            logger.warning(
                f"Some dates could not be parsed and are set to NaT: {failed_dates}"
            )

        logger.debug("'date' column has been formatted.")
    except Exception as e:
        logger.error(f"Error formatting 'date' column: {e}", exc_info=True)

    return df


def validate_dates(
    df: pd.DataFrame, start_year: int, end_year: int
) -> pd.DataFrame:
    """
    Validate that dates fall within the specified year range.

    Args:
        df (pd.DataFrame): DataFrame with a 'date' column.
        start_year (int): Start year for validation.
        end_year (int): End year for validation.

    Returns:
        pd.DataFrame: DataFrame with dates validated.
    """
    try:
        initial_count = len(df)
        df = df[df["date"].dt.year.between(start_year, end_year)]
        final_count = len(df)
        logger.info(
            f"Validated dates. Rows before: {initial_count}, after: {final_count}."
        )
    except Exception as e:
        logger.error(f"Error validating dates: {e}", exc_info=True)
    return df


def get_year(pdf_path: str) -> Optional[int]:
    """
    Extract the year from the PDF using the extract_version_from_pdf function.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        Optional[int]: Extracted year or None if not found.
    """
    try:
        version_data = extract_version_from_pdf(pdf_path)

        if isinstance(version_data, str):
            logger.debug(
                "Extracted version data is a string. Attempting regex search for year."
            )
            year_match = re.search(r"\b(20\d{2})\b", version_data)
            if year_match:
                year = int(year_match.group(1))
                logger.info(f"Extracted year from string: {year}")
                return year
            else:
                logger.warning(
                    f"No valid year found in version string: '{version_data}'"
                )
                return None

        elif isinstance(version_data, datetime):
            year = version_data.year
            logger.info(f"Extracted year from datetime object: {year}")
            return year

        else:
            logger.warning(
                "Version data is neither a string nor a datetime object."
            )
            return None
    except Exception as e:
        logger.error(
            f"Error extracting year from PDF '{pdf_path}': {e}", exc_info=True
        )
        return None


# ================================
# OpenAI Parsing Functions
# ================================


def openai_parser(api_key: str, details: str) -> List[Dict[str, Any]]:
    """
    Parse complex multi-line timetable event details into structured JSON using OpenAI API.

    Args:
        api_key (str): OpenAI API key.
        details (str): Raw event details to parse.

    Returns:
        List[Dict[str, Any]]: List of parsed event dictionaries.
    """
    openai.api_key = api_key
    failure_response = [
        {
            "course": "!!! AiParsing Failure!!!",
            "lecturer": [],
            "location": "",
            "details": "",
        }
    ]
    system_prompt = (
        "You are provided with event details from a timetable, including course names, lecturers, "
        "locations, and additional details. Your task is to parse these details into a structured JSON "
        "format compliant with RFC8259, where each JSON object includes only 'course', 'lecturer', 'location', "
        "and 'details'. The 'lecturer' field should be an array containing multiple names, regardless of their "
        "position in the input. Here is a list of some existing names: ['Herth', 'Wetter', 'Battermann', "
        "'P. Wette', 'Luhmeyer', 'Schünemann', 'P. Wette', 'Simon']. Ensure no additional fields are introduced. "
        "For example, if the input is 'Programmieren in C, P. Wette/ D 216 Praktikum 1, Gr. B Simon "
        "Wechselstromtechnik Battermann/ D 221 Praktikum 2, Gr. A Schünemann', the output should be "
        "[{'course': 'Programmieren in C', 'lecturer': ['P. Wette', 'Simon'], 'location': 'D 216', 'details': 'Praktikum 1, Gr. B'}, "
        "{'course': 'Wechselstromtechnik', 'lecturer': ['Battermann', 'Schünemann'], 'location': 'D 221', 'details': 'Praktikum 2, Gr. A'}]. "
        "Correctly identify and include all lecturers, even if they appear after location or detail descriptions, ensuring accurate and comprehensive "
        "data representation in each event."
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": details},
    ]

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"OpenAI parsing attempt {attempt} of {max_retries}.")
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=messages,
                temperature=0,
                max_tokens=512,
                top_p=1,
            )
            structured_response = response.choices[0].message.content.strip()

            if not structured_response:
                logger.warning(
                    "Received empty response from OpenAI. Retrying..."
                )
                continue

            structured_data = json.loads(structured_response)
            logger.info("Successfully parsed response from OpenAI.")

            if isinstance(structured_data, dict):
                return [structured_data]
            elif isinstance(structured_data, list):
                return structured_data
            else:
                logger.warning(
                    "Parsed data is neither a list nor a dict. Returning failure response."
                )
                return failure_response

        except json.JSONDecodeError as e:
            logger.warning(
                f"Attempt {attempt}: JSON decode error: {e}. Retrying..."
            )
        except openai.error.OpenAIError as e:
            logger.error(
                f"Attempt {attempt}: OpenAI API error: {e}. Retrying..."
            )
        except Exception as e:
            logger.error(
                f"Attempt {attempt}: Unexpected error: {e}. Retrying...",
                exc_info=True,
            )

        # Exponential backoff
        backoff_time = 2**attempt
        logger.info(f"Waiting for {backoff_time} seconds before retrying...")
        import time

        time.sleep(backoff_time)

    logger.critical(
        "Failed to parse details using OpenAI after multiple attempts."
    )
    return failure_response


# ================================
# Data Processing Functions
# ================================


def convert_raw_event_data_to_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the 'raw_details' column from strings to lists and clean special characters.

    Args:
        df (pd.DataFrame): DataFrame with 'raw_details' column.

    Returns:
        pd.DataFrame: DataFrame with 'raw_details' as lists.
    """
    try:
        logger.info(
            "Converting 'raw_details' from strings to lists and cleaning special characters."
        )
        df["raw_details"] = (
            df["raw_details"]
            .str.split("\n")
            .apply(
                lambda x: [detail.strip().replace("\xa0", " ") for detail in x]
                if isinstance(x, list)
                else x
            )
        )
        logger.debug("'raw_details' column has been converted to lists.")
    except Exception as e:
        logger.error(
            f"Error converting 'raw_details' to lists: {e}", exc_info=True
        )
    return df


def check_multievent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check and flag rows that contain multiple events.

    Args:
        df (pd.DataFrame): DataFrame with 'raw_details' column.

    Returns:
        pd.DataFrame: DataFrame with 'multi_event' flag.
    """
    try:
        logger.info("Checking for rows with multiple events.")
        df["multi_event"] = df["raw_details"].apply(
            lambda x: isinstance(x, list) and len(x) > 4
        )
        multi_event_count = df["multi_event"].sum()
        logger.info(f"Found {multi_event_count} rows with multiple events.")
    except Exception as e:
        logger.error(f"Error checking for multiple events: {e}", exc_info=True)
        df["multi_event"] = False
    return df


def process_data(df: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """
    Process the DataFrame to extract structured event data.

    Args:
        df (pd.DataFrame): DataFrame to process.
        api_key (str): OpenAI API key.

    Returns:
        pd.DataFrame: Processed DataFrame with structured event data.
    """
    processed_events_columns = [
        "date",
        "start_time",
        "end_time",
        "course",
        "lecturer",
        "location",
        "details",
    ]
    processed_events: List[Dict[str, Any]] = []

    logger.info("Starting processing of event data.")
    for index, row in df.iterrows():
        raw_details = row.get("raw_details")
        logger.debug(f"Processing row {index}: {row.to_dict()}")

        if row.get("multi_event"):
            logger.info(
                f"Row {index} identified as multi-event. Invoking OpenAI parser."
            )
            details_string = ", ".join(filter(None, raw_details))
            parsed_events = openai_parser(api_key, details_string)
            logger.debug(f"Parsed events for row {index}: {parsed_events}")

            for event in parsed_events:
                if isinstance(event, dict):
                    processed_event = {
                        "date": row.get("date"),
                        "start_time": row.get("start_time"),
                        "end_time": row.get("end_time"),
                        "course": event.get("course", "Unknown Course"),
                        "lecturer": event.get(
                            "lecturer", ["Unknown Lecturer"]
                        ),
                        "location": event.get("location", "Unknown Location"),
                        "details": event.get("details", ""),
                    }
                    processed_events.append(processed_event)
                    logger.info(
                        f"Added parsed event from row {index}: {processed_event}"
                    )
                else:
                    logger.warning(
                        f"Unexpected event format in row {index}: {event}"
                    )
        else:
            if isinstance(raw_details, list):
                event = {
                    "date": row.get("date"),
                    "start_time": row.get("start_time"),
                    "end_time": row.get("end_time"),
                    "course": raw_details[0]
                    if len(raw_details) > 0
                    else "Unknown Course",
                    "lecturer": [raw_details[1]]
                    if len(raw_details) > 1
                    else ["Unknown Lecturer"],
                    "location": raw_details[2]
                    if len(raw_details) > 2
                    else "Unknown Location",
                    "details": raw_details[3] if len(raw_details) > 3 else "",
                }
                processed_events.append(event)
                logger.info(f"Added single event from row {index}: {event}")
            else:
                logger.warning(
                    f"Expected 'raw_details' to be a list in row {index}, got {type(raw_details)}."
                )

    processed_df = pd.DataFrame(
        processed_events, columns=processed_events_columns
    )
    logger.info(
        f"Processing completed. Total processed events: {len(processed_df)}"
    )
    return processed_df


# ================================
# Main Parser Class
# ================================


class PdfParser:
    """
    A class to parse timetable PDFs and extract structured event data.
    """

    def __init__(
        self,
        api_key: str,
        start_year: int = 2024,
        end_year: int = 2025,
        output_dir: str = "output",
    ):
        """
        Initialize the PdfParser.

        Args:
            api_key (str): OpenAI API key.
            start_year (int, optional): Start year for date validation. Defaults to 2024.
            end_year (int, optional): End year for date validation. Defaults to 2025.
            output_dir (str, optional): Directory to save outputs. Defaults to "output".
        """
        self.api_key = api_key
        self.start_year = start_year
        self.end_year = end_year
        self.output_dir = output_dir

        # Ensure the output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        logger.debug(
            f"PdfParser initialized with output directory: {self.output_dir}"
        )

    def parse_pdf(
        self,
        pdf_path: str,
        save_raw: bool = False,
        save_csv_events: bool = False,
        save_json_events: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Parse the PDF and extract structured event data.

        Args:
            pdf_path (str): Path to the PDF file.
            save_raw (bool, optional): Whether to save raw tables. Defaults to False.
            save_csv_events (bool, optional): Whether to save the events DataFrame as CSV. Defaults to False.

        Returns:
            Optional[pd.DataFrame]: Structured event DataFrame or None if processing fails.
        """
        logger.info(f"Initiating parsing process for PDF: {pdf_path}")
        raw_tables = extract_tables(pdf_path)

        if not raw_tables:
            logger.error(f"No tables extracted from PDF: {pdf_path}")
            return None

        if save_raw:
            logger.info("Saving raw tables as per user request.")
            save_raw_tables(raw_tables, self.output_dir)

        df = convert_tablelist_to_dataframe(raw_tables)
        if df.empty:
            logger.error(
                "Conversion resulted in an empty DataFrame. Aborting processing."
            )
            return None

        df = melt_df(df)
        df = forward_fill_dates(df)
        df = df[df["raw_details"].notna() & (df["raw_details"] != "")]
        df = clean_special_chars(df)
        df = clean_time_slot(df)
        df = split_time_slot(df)
        df = convert_raw_event_data_to_list(df)
        year = get_year(pdf_path)

        if year:
            df = format_date(df, year)
            df = validate_dates(
                df, start_year=self.start_year, end_year=self.end_year
            )
        else:
            logger.warning("Year extraction failed. Skipping date formatting.")

        df = df.sort_values(by=["date", "start_time"]).reset_index(drop=True)
        df = check_multievent(df)

        if save_csv_events:
            base_name = os.path.splitext(os.path.basename(pdf_path))[0]
            output_csv_filename = f"{base_name}_events.csv"
            output_csv_path = os.path.join(
                self.output_dir, output_csv_filename
            )
            try:
                save_to_csv(df, output_csv_path)
                logger.info(f"Processed events saved to '{output_csv_path}'.")# TODO: Add json
            except Exception as e:
                logger.error(
                    f"Failed to save processed events to CSV: {e}",
                    exc_info=True,
                )
        if save_json_events:
            base_name = os.path.splitext(os.path.basename(pdf_path))[0]
            output_json_filename = f"{base_name}_events.json"
            output_json_path = os.path.join(
                self.output_dir, output_json_filename
            )
            try:
                save_events_to_json(df, output_json_path)
                logger.info(f"Processed events saved to '{output_json_path}'.")
            except Exception as e:
                logger.error(
                    f"Failed to save processed events to JSON: {e}",
                    exc_info=True,
                )
        logger.info("PDF parsing process completed successfully.")
        return df


def main():
    # Load configuration
    config = load_config("/workspaces/py.hsbi-timetable_2.0/config/config.yaml")
    api_key = config.get("openai", {}).get("api_key")
    output_dir = config.get("output_dir", "output")

    if not api_key:
        logger.critical("OpenAI API key not found in the configuration.")
        return

    # Path to the PDF file
    pdf_path = "downloads/timetable_elm_3/2024-10-11_09-25-00/Stundenplan WS_2024_2025_ELM 3_Stand 2024-10-11.pdf"

    # Initialize the parser
    parser = PdfParser(api_key=api_key, output_dir=output_dir)

    # Parse the PDF
    df = parser.parse_pdf(pdf_path, save_raw=False, save_csv_events=True)

    # Check and use the DataFrame
    if df is not None and not df.empty:
        logger.info("Parsed DataFrame:")
        logger.info(df.head())
    else:
        logger.error("Parsing failed or resulted in an empty DataFrame.")


if __name__ == "__main__":
    main()
