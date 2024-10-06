import logging
import pandas as pd
import yaml
import os

logger = logging.getLogger(__name__)


def init_ghostscript_via_brew_on_mac():
    """
    Initialize the environment by setting up necessary paths for Ghostscript.
    This sets the PATH and DYLD_LIBRARY_PATH environment variables to include
    Ghostscript's bin and lib directories.
    """
    
    # Define Ghostscript bin and lib paths
    GS_BIN_PATH = "/opt/homebrew/bin"
    GS_LIB_PATH = "/opt/homebrew/opt/ghostscript/lib"

    try:
        # Update the PATH environment variable
        os.environ["PATH"] = f"{GS_BIN_PATH}{os.pathsep}{os.environ.get('PATH', '')}"
        logging.info(f"Updated PATH environment variable to include: {GS_BIN_PATH}")

        # Update the DYLD_LIBRARY_PATH environment variable
        os.environ["DYLD_LIBRARY_PATH"] = f"{GS_LIB_PATH}{os.pathsep}{os.environ.get('DYLD_LIBRARY_PATH', '')}"
        logging.info(f"Updated DYLD_LIBRARY_PATH environment variable to include: {GS_LIB_PATH}")
    except Exception as e:
        logging.error(f"Failed to initialize Ghostscript environment: {e}")



def read_csv(input_path):
    try:
        df = pd.read_csv(input_path)
        logging.info(f"Successfully read data from: {input_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from {input_path}: {e}")
        return None


def save_to_csv(df, path):
    try:
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(path), exist_ok=True)

        df.to_csv(path, index=False)
        logging.info(f"Data successfully saved to {path}")
    except Exception as e:
        logging.error(f"Failed to save data to {path}: {e}")


def load_config(filename="config/config.yaml"):
    """Load config from a YAML file."""
    with open(filename, "r") as file:
        # Load the YAML file
        config = yaml.safe_load(file)
    return config


def save_events_to_json(df, output_path):
    """Save the extracted events to a JSON file."""
    try:
        df.to_json(output_path, orient="records", lines=False)
        logging.info(f"Successfully saved DataFrame to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save DataFrame to JSON: {e}")


if __name__ == "__main__":
    # Example usage
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    output_path = "./output/test_data.csv"
    save_to_csv(df, output_path)
    config = load_config()
    print(config)
