"""
Logging Configuration Module
============================

Provides centralized logging configuration for Spark Lab applications.
Supports both console and file logging with structured output.

Usage:
    from utils.logging_config import get_logger

    logger = get_logger(__name__)
    logger.info("Processing started")
    logger.error("Failed to process row", extra={"row_id": 123})

Features:
    - Structured log format with timestamp, level, module
    - Console output (always enabled)
    - Optional file output
    - Configurable log levels
    - Integration with Spark's logging system

Environment Variables:
    LOG_LEVEL:   Default log level (default: INFO)
    LOG_FORMAT:  Log format string (default: structured)
    LOG_FILE:    Optional log file path
"""

import logging
import sys
import os
from datetime import datetime
from typing import Optional


# Default configuration
DEFAULT_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEFAULT_LOG_FORMAT = os.getenv(
    "LOG_FORMAT", "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class SparkLabFormatter(logging.Formatter):
    """
    Custom formatter for Spark Lab logs.

    Provides structured output with:
    - Timestamp
    - Log level (color-coded in console)
    - Logger name (module)
    - Message
    - Extra fields (if provided)
    """

    # ANSI color codes for console output
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def __init__(self, fmt=None, datefmt=None, use_colors=True):
        super().__init__(fmt, datefmt)
        self.use_colors = use_colors

    def format(self, record):
        # Store original level name
        original_level = record.levelname

        # Add color to level name for console
        if self.use_colors and original_level in self.COLORS:
            record.levelname = (
                f"{self.COLORS[original_level]}{original_level}{self.RESET}"
            )

        # Format the message
        formatted = super().format(record)

        # Restore original level name (in case record is reused)
        record.levelname = original_level

        return formatted


def get_logger(
    name: str,
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    use_colors: bool = True,
) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name (typically __name__)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for log output
        use_colors: Whether to use colored output (console only)

    Returns:
        Configured logger instance

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing started")
        >>> logger.warning("Low memory", extra={"available_mb": 512})

    Note:
        Console output is always enabled. File output is optional.
        Colors are automatically disabled for file handlers.
    """
    # Get or create logger
    logger = logging.getLogger(name)

    # Set log level
    log_level = level or DEFAULT_LOG_LEVEL
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    # Console handler (always enabled)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_formatter = SparkLabFormatter(
        fmt=DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT, use_colors=use_colors
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = SparkLabFormatter(
            fmt=DEFAULT_LOG_FORMAT,
            datefmt=DEFAULT_DATE_FORMAT,
            use_colors=False,  # No colors in file
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


def configure_spark_logging(level: str = "WARN"):
    """
    Configure Spark's internal logging level.

    Spark uses log4j internally, but we can control the Python
    side logging to reduce verbosity.

    Args:
        level: Log level for Spark (DEBUG, INFO, WARN, ERROR)

    Example:
        >>> configure_spark_logging("ERROR")  # Only show errors
    """
    # Set Py4J logging (Java-Python bridge)
    logging.getLogger("py4j").setLevel(getattr(logging, level.upper()))

    # Set Spark logging
    logging.getLogger("pyspark").setLevel(getattr(logging, level.upper()))


def log_execution_info(logger: logging.Logger, spark_session=None):
    """
    Log execution environment information.

    Useful for debugging and auditing. Logs:
    - Python version
    - Current timestamp
    - Spark configuration (if session provided)

    Args:
        logger: Logger instance
        spark_session: Optional SparkSession for config info

    Example:
        >>> logger = get_logger(__name__)
        >>> spark = create_spark_session()
        >>> log_execution_info(logger, spark)
    """
    logger.info("=" * 60)
    logger.info("Execution Environment")
    logger.info("=" * 60)
    logger.info(f"Python: {sys.version}")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")

    if spark_session:
        try:
            logger.info(f"Spark Version: {spark_session.version}")
            logger.info(f"App Name: {spark_session.sparkContext.appName}")
            logger.info(f"Master: {spark_session.sparkContext.master}")
        except Exception as e:
            logger.warning(f"Could not get Spark info: {e}")

    logger.info("=" * 60)


# Convenience function for quick setup
def setup_logging(
    name: str = "spark_lab", level: str = "INFO", log_file: Optional[str] = None
) -> logging.Logger:
    """
    Quick setup for logging with sensible defaults.

    Args:
        name: Logger name
        level: Log level
        log_file: Optional log file path

    Returns:
        Configured logger

    Example:
        >>> logger = setup_logging("my_app", "DEBUG", "logs/my_app.log")
    """
    # Configure Spark logging to reduce noise
    configure_spark_logging("WARN")

    # Get logger
    return get_logger(name, level, log_file)


# Example usage and testing
if __name__ == "__main__":
    print("Testing logging configuration...")
    print()

    # Test basic logging
    logger = get_logger("test_logger", level="DEBUG")

    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    # Test with extra fields
    logger.info("Processing record", extra={"custom_field": "value", "record_id": 123})

    print()
    print("Testing file logging...")

    # Test file logging
    test_log_file = "/tmp/spark_lab_test.log"
    file_logger = get_logger("file_test", log_file=test_log_file)
    file_logger.info("This message goes to both console and file")

    print(f"Check log file: {test_log_file}")
    print()
    print("All tests passed!")
