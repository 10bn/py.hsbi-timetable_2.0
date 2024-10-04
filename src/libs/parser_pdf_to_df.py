import logging
import os
import camelot
import pandas as pd
from timetable_version import extract_version
from utils import save_to_csv
from logger import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

def extract_tables(pdf_path):
    try:
        logger.info(f"Starting to extract tables from: {pdf_path}")
        table_list = camelot.read_pdf(pdf_path, flavor="lattice", pages="all")
        logger.info(f"Successfully extracted {len(table_list)} tables.")
        return table_list
    except Exception as e:
        logger.error(f"Failed to extract tables: {e}")
        return None

def save_raw_tables(table_list, output_dir):
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

def convert_tablelist_to_dataframe(df_tables):
    try:
        dataframes = [
            table.df if i == 0 else table.df.iloc[1:]
            for i, table in enumerate(df_tables)
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
    try:
        df = df.melt(id_vars=["date"], var_name="time_slot", value_name="raw_details")
        logger.debug(f"DataFrame after melting has shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error melting DataFrame: {e}")
        return pd.DataFrame()

def get_year(pdf_path):
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

def format_date(df, current_year):
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

def clean_special_chars(df):
    """
    Clean special characters in the entire DataFrame.
    """
    try:
        df = df.applymap(lambda x: x.replace('\xa0', ' ') if isinstance(x, str) else x)
        df = df.applymap(lambda x: x.replace('‐', '-') if isinstance(x, str) else x)
        logger.info("Special characters cleaned successfully.")
    except Exception as e:
        logger.error(f"Error cleaning special characters: {e}")
    return df

def split_time_slot(df):
    if "time_slot" in df.columns:
        logger.info("Splitting 'time_slot' column into 'start_time' and 'end_time' and replacing 'time_slot'.")
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

def convert_raw_event_data_to_list(df):
    try:
        df["raw_details"] = df["raw_details"].str.split("\n")
        df["raw_details"] = df["raw_details"].apply(lambda x: [detail.strip().replace('\xa0', ' ') for detail in x] if isinstance(x, list) else x)
        logger.debug("Converted 'raw_details' to lists and cleaned special characters.")
        return df
    except Exception as e:
        logger.error(f"Error converting raw details to list: {e}")
        return df

def check_multievent(df):
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

def forward_fill_dates(df):
    df['date'] = df['date'].fillna(method='ffill')
    logger.debug("Forward filled missing dates.")
    return df

def clean_time_slot(df):
    if "time_slot" in df.columns:
        df["time_slot"] = df["time_slot"].str.replace(" Uhr", "", regex=False)
        logger.debug("Cleaned 'time_slot' column.")
    return df

def validate_dates(df, start_year, end_year):
    initial_count = df.shape[0]
    df = df[(df['date'].dt.year >= start_year) & (df['date'].dt.year <= end_year)]
    final_count = df.shape[0]
    logger.info(f"Validated dates. Rows before: {initial_count}, after: {final_count}")
    return df

def create_df_from_pdf(pdf_path, save_raw=False, output_dir="output"):
    raw_data = extract_tables(pdf_path)

    if raw_data:
        if save_raw:
            save_raw_tables(raw_data, output_dir)

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
            df = validate_dates(df, start_year=2024, end_year=2025)
        else:
            logger.warning("Year information is missing. Skipping date formatting.")

        df = df.sort_values(by=["date", "start_time"])
        df = check_multievent(df)

        output_csv_path = os.path.join(output_dir, "create_df.csv")
        save_to_csv(df, output_csv_path)
        logger.info(f"Final DataFrame saved to {output_csv_path}")
        return df
    else:
        logger.error(f"Failed to create DataFrame from PDF: {pdf_path}")
        return None

if __name__ == "__main__":
    pdf_path = "downloads/timetable_1/Stundenplan WS_2024_2025_ELM 3.pdf"
    df = create_df_from_pdf(pdf_path, save_raw=False)