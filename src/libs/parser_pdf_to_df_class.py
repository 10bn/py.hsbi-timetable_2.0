import logging
import os
from typing import Optional

import camelot
import pandas as pd

from timetable_version import extract_version
from utils import save_to_csv
from logger import setup_logger


class TimetableProcessor:
    def __init__(self, pdf_path: str, save_raw: bool = False, output_dir: str = "output"):
        self.pdf_path = pdf_path
        self.save_raw = save_raw
        self.output_dir = output_dir
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        setup_logger()
        return logging.getLogger(__name__)

    def extract_tables(self) -> Optional[list]:
        try:
            self.logger.info(f"Starting to extract tables from: {self.pdf_path}")
            tables = camelot.read_pdf(self.pdf_path, flavor="lattice", pages="all")
            self.logger.info(f"Successfully extracted {len(tables)} tables.")
            return tables
        except Exception as e:
            self.logger.error(f"Failed to extract tables: {e}")
            return None

    def save_raw_tables(self, tables: list) -> None:
        try:
            if not tables:
                self.logger.warning("No tables to save.")
                return

            raw_output_dir = os.path.join(self.output_dir, "raw_tables")
            os.makedirs(raw_output_dir, exist_ok=True)
            self.logger.info(f"Saving raw tables to directory: {raw_output_dir}")

            for idx, table in enumerate(tables, start=1):
                table_filename = os.path.join(raw_output_dir, f"raw_table_{idx}.csv")
                table.to_csv(table_filename, index=False)
                self.logger.debug(f"Saved raw table {idx} to {table_filename}")

            self.logger.info(f"Successfully saved {len(tables)} raw tables.")
        except Exception as e:
            self.logger.error(f"Failed to save raw tables: {e}")

    def convert_tables_to_dataframe(self, tables: list) -> pd.DataFrame:
        try:
            self.logger.info("Converting tables to DataFrame.")
            dataframes = [
                table.df if i == 0 else table.df.iloc[1:]
                for i, table in enumerate(tables)
            ]
            df_final = pd.concat(dataframes, ignore_index=True)
            new_header = df_final.iloc[0]
            df_final = df_final[1:]
            df_final.columns = new_header
            df_final.drop(df_final.columns[0], axis=1, inplace=True)
            df_final.rename(columns={df_final.columns[0]: "date"}, inplace=True)

            self.logger.debug(f"Converted tables to DataFrame with shape: {df_final.shape}")
            return df_final
        except Exception as e:
            self.logger.error(f"Error converting table list to DataFrame: {e}")
            return pd.DataFrame()

    def melt_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info("Melting DataFrame.")
            df_melted = df.melt(id_vars=["date"], var_name="time_slot", value_name="raw_details")
            self.logger.debug(f"DataFrame after melting has shape: {df_melted.shape}")
            return df_melted
        except Exception as e:
            self.logger.error(f"Error melting DataFrame: {e}")
            return pd.DataFrame()

    def get_year_from_pdf(self) -> Optional[int]:
        try:
            version_datetime = extract_version(self.pdf_path)
            if version_datetime:
                self.logger.debug(f"Extracted version year: {version_datetime.year}")
                return version_datetime.year
            self.logger.warning("Version datetime not found.")
            return None
        except Exception as e:
            self.logger.error(f"Error extracting year: {e}")
            return None

    def format_dates(self, df: pd.DataFrame, current_year: int) -> pd.DataFrame:
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
        self.logger.info(f"Starting to format dates with the year: {current_year_str}")

        try:
            df["date"] = df["date"].replace(month_mapping, regex=True)
            df["date"] = pd.to_datetime(
                df["date"].astype(str) + " " + current_year_str,
                format="%d. %b %Y",
                errors="coerce",
            )

            if df["date"].isna().any():
                failed_dates = df[df["date"].isna()]["date"]
                self.logger.warning(
                    f"Some dates were not parsed correctly and have been set to NaT. Check these entries: {failed_dates.to_list()}"
                )

            self.logger.info("Dates formatted successfully.")
        except Exception as e:
            self.logger.error(f"Error formatting dates: {e}")
        return df

    def clean_special_characters(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Cleaning special characters in DataFrame.")
        try:
            df = df.applymap(self._replace_non_breaking_space)
            df = df.applymap(self._replace_hyphen)
            self.logger.info("Special characters cleaned successfully.")
        except Exception as e:
            self.logger.error(f"Error cleaning special characters: {e}")
        return df

    @staticmethod
    def _replace_non_breaking_space(value: str) -> str:
        return value.replace('\xa0', ' ') if isinstance(value, str) else value

    @staticmethod
    def _replace_hyphen(value: str) -> str:
        return value.replace('‐', '-') if isinstance(value, str) else value

    def split_time_slot(self, df: pd.DataFrame) -> pd.DataFrame:
        if "time_slot" not in df.columns:
            self.logger.warning("The column 'time_slot' does not exist in the DataFrame. No action taken.")
            return df

        self.logger.info("Splitting 'time_slot' column into 'start_time' and 'end_time'.")
        try:
            df["time_slot"] = df["time_slot"].str.replace(" Uhr", "", regex=False)
            self.logger.debug(f"Cleaned time_slot values: {df['time_slot'].head()}")

            time_splits = df["time_slot"].str.split(pat=" - ", n=1, expand=True)
            time_splits = time_splits.rename(columns={0: "start_time_str", 1: "end_time_str"})

            df["start_time"] = pd.to_datetime(
                time_splits["start_time_str"].str.strip(), format="%H.%M", errors="coerce"
            ).dt.time
            df["end_time"] = pd.to_datetime(
                time_splits["end_time_str"].str.strip(), format="%H.%M", errors="coerce"
            ).dt.time

            self._log_time_parsing_warnings(df)

            # Insert 'start_time' and 'end_time' before 'time_slot'
            time_slot_idx = df.columns.get_loc("time_slot")
            df.insert(time_slot_idx, "start_time", df.pop("start_time"))
            df.insert(time_slot_idx + 1, "end_time", df.pop("end_time"))
            df.drop("time_slot", axis=1, inplace=True)

            self.logger.info("Successfully replaced 'time_slot' with 'start_time' and 'end_time'.")
        except Exception as e:
            self.logger.error(f"Error processing 'time_slot': {e}")
            self.logger.debug(f"Current time_slot values (post-error): {df.get('time_slot', pd.Series()).head()}")
        return df

    def _log_time_parsing_warnings(self, df: pd.DataFrame) -> None:
        if df["start_time"].isna().any():
            problematic_slots = df[df["start_time"].isna()]["time_slot"]
            self.logger.warning(f"Failed to parse 'start_time' for some entries: {problematic_slots.tolist()}")

        if df["end_time"].isna().any():
            problematic_slots = df[df["end_time"].isna()]["time_slot"]
            self.logger.warning(f"Failed to parse 'end_time' for some entries: {problematic_slots.tolist()}")

    def convert_raw_details_to_list(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info("Converting 'raw_details' to lists and cleaning special characters.")
            df["raw_details"] = df["raw_details"].str.split("\n")
            df["raw_details"] = df["raw_details"].apply(
                lambda x: [detail.strip().replace('\xa0', ' ') for detail in x] if isinstance(x, list) else x
            )
            self.logger.debug("'raw_details' converted to lists.")
            return df
        except Exception as e:
            self.logger.error(f"Error converting raw details to list: {e}")
            return df

    def check_multiple_events(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info("Checking for multiple events in 'raw_details'.")
            df["multi_event"] = df['raw_details'].apply(
                lambda x: len(x) > 4 if isinstance(x, list) else False
            )
            multi_event_count = df['multi_event'].sum()
            self.logger.info(f"Number of multi-event rows: {multi_event_count}")
            return df
        except Exception as e:
            self.logger.error(f"Error checking for multiple events: {e}")
            return df

    def forward_fill_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info("Forward filling missing dates.")
            df['date'] = df['date'].fillna(method='ffill')
            self.logger.debug("Missing dates forward filled.")
        except Exception as e:
            self.logger.error(f"Error forward filling dates: {e}")
        return df

    def clean_time_slot_column(self, df: pd.DataFrame) -> pd.DataFrame:
        if "time_slot" in df.columns:
            self.logger.info("Cleaning 'time_slot' column.")
            try:
                df["time_slot"] = df["time_slot"].str.replace(" Uhr", "", regex=False)
                self.logger.debug("'time_slot' column cleaned.")
            except Exception as e:
                self.logger.error(f"Error cleaning 'time_slot' column: {e}")
        else:
            self.logger.warning("The column 'time_slot' does not exist in the DataFrame. No action taken.")
        return df

    def validate_dates(self, df: pd.DataFrame, start_year: int, end_year: int) -> pd.DataFrame:
        try:
            initial_count = df.shape[0]
            df = df[(df['date'].dt.year >= start_year) & (df['date'].dt.year <= end_year)]
            final_count = df.shape[0]
            self.logger.info(f"Validated dates. Rows before: {initial_count}, after: {final_count}")
        except Exception as e:
            self.logger.error(f"Error validating dates: {e}")
        return df

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.forward_fill_dates(df)
        df = df[df["raw_details"] != ""]
        df = self.clean_special_characters(df)
        df = self.clean_time_slot_column(df)
        df = self.split_time_slot(df)
        df = self.convert_raw_details_to_list(df)
        return df

    def create_dataframe(self) -> Optional[pd.DataFrame]:
        tables = self.extract_tables()
        if not tables:
            self.logger.error(f"Failed to extract tables from PDF: {self.pdf_path}")
            return None

        if self.save_raw:
            self.save_raw_tables(tables)

        df = self.convert_tables_to_dataframe(tables)
        if df.empty:
            self.logger.error("Converted DataFrame is empty.")
            return None

        df = self.melt_dataframe(df)
        df = self.clean_dataframe(df)

        year = self.get_year_from_pdf()
        if year:
            df = self.format_dates(df, year)
            df = self.validate_dates(df, start_year=2024, end_year=2025)
        else:
            self.logger.warning("Year information is missing. Skipping date formatting.")

        df = df.sort_values(by=["date", "start_time"])
        df = self.check_multiple_events(df)

        output_csv_path = os.path.join(self.output_dir, "create_df.csv")
        save_to_csv(df, output_csv_path)
        self.logger.info(f"Final DataFrame saved to {output_csv_path}")
        return df


def main():
    pdf_path = "downloads/timetable_1/Stundenplan WS_2024_2025_ELM 3.pdf"
    processor = TimetableProcessor(pdf_path=pdf_path, save_raw=False)
    processor.create_dataframe()


if __name__ == "__main__":
    main()
