# src/libs/logger.py
import logging
from logging.handlers import RotatingFileHandler

# Constants for better maintainability
LOG_FILE = "logfile.log"
LOG_FORMAT_FILE = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(lineno)d)"
LOG_FORMAT_STREAM = "%(asctime)s - %(name)s - %(levelname)s:\n  %(message)s"

def setup_logger(log_level: int = logging.INFO, log_to_console: bool = True) -> None:
    """
    Set up the root logger with the specified log level.

    Args:
        log_level (int, optional): Logging level (e.g., logging.DEBUG, logging.INFO). Defaults to logging.INFO.
        log_to_console (bool, optional): Whether to add a console stream handler. Defaults to True.
    """
    logger = logging.getLogger()
    
    # Prevent adding multiple handlers if already configured
    if not logger.hasHandlers():
        logger.setLevel(log_level)
        
        # Rotating File Handler
        file_handler = RotatingFileHandler(
            LOG_FILE, maxBytes=5*1024*1024, backupCount=3
        )  # 5MB per file, keep 3 backups
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(LOG_FORMAT_FILE)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        if log_to_console:
            # Stream Handler
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(log_level)
            stream_formatter = logging.Formatter(LOG_FORMAT_STREAM)
            stream_handler.setFormatter(stream_formatter)
            logger.addHandler(stream_handler)

# Example usage
if __name__ == "__main__":
    setup_logger(log_level=logging.WARNING, log_to_console=True)
    logger = logging.getLogger(__name__)
    logger.warning("Logger has been set up with default WARNING level.")
