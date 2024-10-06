# check_gs.py

import subprocess
import logging
import os

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def check_ghostscript():
    """
    Verify that Ghostscript is accessible from Python.
    """
    try:
        result = subprocess.run(["gs", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            gs_version = result.stdout.strip()
            logger.info(f"Ghostscript is accessible. Version: {gs_version}")
        else:
            logger.error("Ghostscript is not accessible.")
    except FileNotFoundError:
        logger.error("Ghostscript executable not found.")
    except Exception as e:
        logger.error(f"An error occurred while checking Ghostscript: {e}")

if __name__ == "__main__":
    # Prepend Ghostscript paths to environment variables
    gs_bin_path = "/opt/homebrew/bin"
    gs_lib_path = "/opt/homebrew/opt/ghostscript/lib"

    os.environ["PATH"] = gs_bin_path + os.pathsep + os.environ.get("PATH", "")
    os.environ["DYLD_LIBRARY_PATH"] = gs_lib_path + os.pathsep + os.environ.get("DYLD_LIBRARY_PATH", "")

    logger.debug(f"Updated PATH: {os.environ['PATH']}")
    logger.debug(f"Updated DYLD_LIBRARY_PATH: {os.environ['DYLD_LIBRARY_PATH']}")

    check_ghostscript()
