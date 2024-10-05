import json
import logging
import os

import camelot
import pandas as pd
from openai import OpenAI

from libs.logger import setup_logger
from libs.timetable_version import extract_version
from libs.utils import save_to_csv, load_config
# Setup logging
setup_logger()
logger = logging.getLogger(__name__)

# ================================
# PDF Parsing Functions
# ================================

def extract_tables(pdf_path):
    """
    Extract tables from a PDF using Camelot.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        list or None: List of extracted tables or None if extraction fails.
    """
    try:
        logger.info(f"Starting to extract tables from: {pdf_path}")
        table_list = camelot.read_pdf(pdf_path, flavor="lattice", pages="all")
        logger.info(f"Successfully extracted {len(table_list)} tables.")
        return table_list
    except Exception as e:
        logger.error(f"Failed to extract tables: {e}")
        return None


def save_raw_tables(table_list, output_dir):
    """
    Save raw tables extracted from PDF to CSV files.

    Args:
        table_list (list): List of tables extracted by Camelot.
        output_dir (str): Directory to save the raw CSV files.
    """
    try:
        if not table_list:
            logger.warning("No tables to save.")
            return

        raw_output_dir = os.path.join(output_dir, "raw_tables")
        os.makedirs(raw_output_dir, exist_ok=True)
        logger.info(f"Saving raw tables to directory: {raw_output_dir}")

        for idx, table in enumerate(table_list, start=1):
            table_filename = os.path.join(raw_output_dir, f"raw_table_{idx}.csv")
            table.to_csv(table_filename, index=False)
            logger.debug(f"Saved raw table {idx} to {table_filename}")

        logger.info(f"Successfully saved {len(table_list)} raw tables.")
    except Exception as e:
        logger.error(f"Failed to save raw tables: {e}")


def convert_tablelist_to_dataframe(table_list):
    """
    Convert a list of Camelot tables to a single pandas DataFrame.

    Args:
        table_list (list): List of tables extracted by Camelot.

    Returns:
        pd.DataFrame: Combined DataFrame from all tables.
    """
    try:
        dataframes = [
            table.df if i == 0 else table.df.iloc[1:]
            for i, table in enumerate(table_list)
        ]
        df_final = pd.concat(dataframes, ignore_index=True)
        new_header = df_final.iloc[0]
        df_final = df_final[1:]
        df_final.columns = new_header
        df_final.drop(df_final.columns[0], axis=1, inplace=True)
        df_final.rename(columns={df_final.columns[0]: "date"}, inplace=True)

        logger.debug(f"Converted tables to DataFrame with shape: {df_final.shape}")
        return df_final
    except Exception as e:
        logger.error(f"Error converting table list to DataFrame: {e}")
        return pd.DataFrame()


def melt_df(df):
    """
    Melt the DataFrame to have 'date', 'time_slot', and 'raw_details' columns.

    Args:
        df (pd.DataFrame): DataFrame to melt.

    Returns:
        pd.DataFrame: Melted DataFrame.
    """
    try:
        df = df.melt(id_vars=["date"], var_name="time_slot", value_name="raw_details")
        logger.debug(f"DataFrame after melting has shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error melting DataFrame: {e}")
        return pd.DataFrame()


def forward_fill_dates(df):
    """
    Forward fill missing dates in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame with potential missing dates.

    Returns:
        pd.DataFrame: DataFrame with forward-filled dates.
    """
    df['date'] = df['date'].fillna(method='ffill')
    logger.debug("Forward filled missing dates.")
    return df


def clean_special_chars(df):
    """
    Clean special characters in the entire DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to clean.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    try:
        df = df.applymap(lambda x: x.replace('\xa0', ' ') if isinstance(x, str) else x)
        df = df.applymap(lambda x: x.replace('‐', '-') if isinstance(x, str) else x)
        logger.info("Special characters cleaned successfully.")
    except Exception as e:
        logger.error(f"Error cleaning special characters: {e}")
    return df


def clean_time_slot(df):
    """
    Clean the 'time_slot' column by removing ' Uhr'.

    Args:
        df (pd.DataFrame): DataFrame containing the 'time_slot' column.

    Returns:
        pd.DataFrame: DataFrame with cleaned 'time_slot'.
    """
    if "time_slot" in df.columns:
        df["time_slot"] = df["time_slot"].str.replace(" Uhr", "", regex=False)
        logger.debug("Cleaned 'time_slot' column.")
    return df


def split_time_slot(df):
    """
    Split the 'time_slot' column into 'start_time' and 'end_time'.

    Args:
        df (pd.DataFrame): DataFrame containing the 'time_slot' column.

    Returns:
        pd.DataFrame: DataFrame with 'start_time' and 'end_time' columns.
    """
    if "time_slot" in df.columns:
        logger.info("Splitting 'time_slot' column into 'start_time' and 'end_time'.")
        try:
            df["time_slot"] = df["time_slot"].str.replace(" Uhr", "", regex=False)
            logger.debug(f"Cleaned time_slot values: {df['time_slot'].head()}")

            time_splits = df["time_slot"].str.split(pat=" - ", n=1, expand=True)

            if time_splits.shape[1] < 2:
                logger.warning("Some 'time_slot' entries do not have an end time. Filling with NaT.")
                time_splits[1] = pd.NA

            df["start_time"] = pd.to_datetime(
                time_splits[0].str.strip(), format="%H.%M", errors="coerce"
            ).dt.time
            df["end_time"] = pd.to_datetime(
                time_splits[1].str.strip(), format="%H.%M", errors="coerce"
            ).dt.time

            if df["start_time"].isna().any():
                problematic_slots = df[df["start_time"].isna()]["time_slot"]
                logger.warning(f"Failed to parse 'start_time' for some entries: {problematic_slots.tolist()}")

            if df["end_time"].isna().any():
                problematic_slots = df[df["end_time"].isna()]["time_slot"]
                logger.warning(f"Failed to parse 'end_time' for some entries: {problematic_slots.tolist()}")

            idx = df.columns.get_loc("time_slot")
            df.insert(idx, "start_time", df.pop("start_time"))
            df.insert(idx + 1, "end_time", df.pop("end_time"))
            df.drop("time_slot", axis=1, inplace=True)

            logger.info("Successfully replaced 'time_slot' with 'start_time' and 'end_time'.")
        except Exception as e:
            logger.error(f"Error processing 'time_slot': {e}")
            logger.debug(f"Current time_slot values (post-error): {df['time_slot'].head()}")
    else:
        logger.warning("The column 'time_slot' does not exist in the DataFrame. No action taken.")
    return df


def format_date(df, current_year):
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
    logger.info(f"Starting to format dates with the year: {current_year_str}")

    try:
        df["date"] = df["date"].replace(month_mapping, regex=True)
        df["date"] = pd.to_datetime(
            df["date"].astype(str) + " " + current_year_str,
            format="%d. %b %Y",
            errors="coerce",
        )

        if df["date"].isna().any():
            failed_dates = df[df["date"].isna()]["date"]
            logger.warning(
                f"Some dates were not parsed correctly and have been set to NaT. Check these entries: {failed_dates.to_list()}"
            )

        logger.info("Dates formatted successfully.")
    except Exception as e:
        logger.error(f"Error formatting dates: {e}")
    return df


def validate_dates(df, start_year, end_year):
    """
    Validate that dates fall within the specified year range.

    Args:
        df (pd.DataFrame): DataFrame with a 'date' column.
        start_year (int): Start year for validation.
        end_year (int): End year for validation.

    Returns:
        pd.DataFrame: DataFrame with dates validated.
    """
    initial_count = df.shape[0]
    df = df[(df['date'].dt.year >= start_year) & (df['date'].dt.year <= end_year)]
    final_count = df.shape[0]
    logger.info(f"Validated dates. Rows before: {initial_count}, after: {final_count}")
    return df


def get_year(pdf_path):
    """
    Extract the year from the PDF using the extract_version function.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        int or None: Extracted year or None if not found.
    """
    try:
        version_datetime = extract_version(pdf_path)
        if version_datetime:
            logger.debug(f"Extracted version year: {version_datetime.year}")
            return version_datetime.year
        logger.warning("Version datetime not found.")
        return None
    except Exception as e:
        logger.error(f"Error extracting year: {e}")
        return None


# ================================
# OpenAI Parsing Functions
# ================================

def openai_parser(api_key, details):
    """
    Parse complex multi-line timetable event details into structured JSON using OpenAI API.

    Args:
        api_key (str): OpenAI API key.
        details (str): Raw event details to parse.

    Returns:
        list: List of parsed event dictionaries.
    """
    client = OpenAI(api_key=api_key)
    failure_response = [{
        "course": "!!! AiParsing Failure!!!",
        "lecturer": [],
        "location": "",
        "details": "",
    }]
    messages = [
        {
            "role": "system",
            "content": (
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
            ),
        },
        {"role": "user", "content": details},
    ]

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=messages,
                temperature=0,
                max_tokens=512,
                top_p=1,
            )
            structured_response = response.choices[0].message.content
            if not structured_response:
                logger.warning("Received no content to parse, attempting retry.")
                continue

            structured_data = json.loads(structured_response)
            logger.info("Successfully parsed the response.")

            if isinstance(structured_data, dict):
                return [structured_data]
            elif isinstance(structured_data, list):
                return structured_data
            else:
                logger.warning("Parsed data is not a list or dict.")
                return failure_response

        except json.JSONDecodeError as e:
            logger.warning(
                f"Retry {attempt + 1}/{max_retries}: Failed to parse JSON response. {str(e)} Trying again."
            )
        except (IndexError, KeyError, Exception) as e:
            logger.error(
                f"Error during parsing: {e}. Attempt {attempt + 1} of {max_retries}."
            )
            if attempt == max_retries - 1:
                logger.critical(
                    "Error parsing details after several attempts, please check the input format and try again."
                )
                return failure_response

    logger.error("Failed to obtain a valid response after multiple attempts.")
    return failure_response


# ================================
# Data Processing Functions
# ================================

def convert_raw_event_data_to_list(df):
    """
    Convert the 'raw_details' column from strings to lists and clean special characters.

    Args:
        df (pd.DataFrame): DataFrame with 'raw_details' column.

    Returns:
        pd.DataFrame: DataFrame with 'raw_details' as lists.
    """
    try:
        df["raw_details"] = df["raw_details"].str.split("\n")
        df["raw_details"] = df["raw_details"].apply(
            lambda x: [detail.strip().replace('\xa0', ' ') for detail in x] 
            if isinstance(x, list) else x
        )
        logger.debug("Converted 'raw_details' to lists and cleaned special characters.")
        return df
    except Exception as e:
        logger.error(f"Error converting raw details to list: {e}")
        return df


def check_multievent(df):
    """
    Check and flag rows that contain multiple events.

    Args:
        df (pd.DataFrame): DataFrame with 'raw_details' column.

    Returns:
        pd.DataFrame: DataFrame with 'multi_event' flag.
    """
    try:
        df["multi_event"] = df['raw_details'].apply(
            lambda x: len(x) > 4 if isinstance(x, list) else False
        )
        multi_event_count = df['multi_event'].sum()
        logger.info(f"Number of multi-event rows: {multi_event_count}")
        return df
    except Exception as e:
        logger.error(f"Error checking for multiple events: {e}")
        return df


def process_data(df, api_key):
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
    processed_events = []

    for _, row in df.iterrows():
        raw_details = row["raw_details"]
        logger.info(f"Processing row: {row.to_dict()}")

        if row["multi_event"]:
            logger.info("Detected multi-event row, invoking openai_parser.")
            details_string = ", ".join(raw_details)
            parsed_events = openai_parser(api_key, details_string)
            logger.info(f"Parsed events: {parsed_events}")

            if isinstance(parsed_events, list):
                for event in parsed_events:
                    if isinstance(event, dict):
                        processed_event = {
                            "date": row["date"],
                            "start_time": row["start_time"],
                            "end_time": row["end_time"],
                            "course": event.get("course", ""),
                            "lecturer": event.get("lecturer", []),
                            "location": event.get("location", ""),
                            "details": event.get("details", ""),
                        }
                        processed_events.append(processed_event)
                        logger.info(f"Added parsed event: {processed_event}")
            else:
                logger.warning(f"Parsed events is not a list: {parsed_events}")
        else:
            event = {
                "date": row["date"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "course": raw_details[0] if len(raw_details) > 0 else "Unknown Course",
                "lecturer": [raw_details[1]] if len(raw_details) > 1 else ["Unknown Lecturer"],
                "location": raw_details[2] if len(raw_details) > 2 else "Unknown Location",
                "details": raw_details[3] if len(raw_details) > 3 else "",
            }

            processed_events.append(event)
            logger.info(f"Added single event: {event}")

    processed_df = pd.DataFrame(processed_events, columns=processed_events_columns)
    logger.info("Completed processing all rows.")
    return processed_df


# ================================
# Main Parser Class
# ================================

class PdfParser:
    """
    A class to parse timetable PDFs and extract structured event data.
    """

    def __init__(self, api_key, start_year=2024, end_year=2025, output_dir="output"):
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
        logger.debug(f"Initialized PdfParser with output directory: {self.output_dir}")

    def parse_pdf(self, pdf_path, save_raw=False, save_csv_events=False):
        """
        Parse the PDF and extract structured event data.

        Args:
            pdf_path (str): Path to the PDF file.
            save_raw (bool, optional): Whether to save raw tables. Defaults to False.
            save_csv_events (bool, optional): Whether to save the events DataFrame as CSV. Defaults to False.

        Returns:
            pd.DataFrame or None: Structured event DataFrame or None if processing fails.
        """
        logger.info(f"Starting PDF parsing for: {pdf_path}")
        raw_data = extract_tables(pdf_path)

        if raw_data:
            if save_raw:
                save_raw_tables(raw_data, self.output_dir)

            to_df = convert_tablelist_to_dataframe(raw_data)
            df = melt_df(to_df)
            df = forward_fill_dates(df)
            df = df[df["raw_details"] != ""]
            df = clean_special_chars(df)
            df = clean_time_slot(df)
            df = split_time_slot(df)
            df = convert_raw_event_data_to_list(df)
            year = get_year(pdf_path)

            if year:
                df = format_date(df, year)
                df = validate_dates(df, start_year=self.start_year, end_year=self.end_year)
            else:
                logger.warning("Year information is missing. Skipping date formatting.")

            df = df.sort_values(by=["date", "start_time"])
            df = check_multievent(df)

            if save_csv_events:
                # Generate a dynamic CSV filename based on the PDF filename
                base_name = os.path.splitext(os.path.basename(pdf_path))[0]
                output_csv_filename = f"{base_name}_events.csv"
                output_csv_path = os.path.join(self.output_dir, output_csv_filename)

                save_to_csv(df, output_csv_path)
                logger.info(f"Final DataFrame saved to {output_csv_path}")

            logger.info("PDF parsing completed successfully.")
            return df
        else:
            logger.error(f"Failed to create DataFrame from PDF: {pdf_path}")
            return None


# ================================
# Main Execution Block
# ================================

if __name__ == "__main__":
    # Example usage
    api_key = load_config("config/config.yaml").get("api_key")
    pdf_path = "downloads/timetable_1/Stundenplan WS_2024_2025_ELM 3.pdf"

    parser = PdfParser(api_key=api_key, output_dir="output")
    df = parser.parse_pdf(pdf_path, save_raw=False, save_csv_events=True)

    if df is not None:
        logger.info("Parsed DataFrame:")
        logger.info(df.head())
    else:
        logger.error("Parsing failed.")
