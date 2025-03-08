import logging
import sys
from datetime import datetime

class LoggerWriter:
    """
    Redirects sys.stdout to a log file and adds timestamps to each line.
    """
    def __init__(self, log_file):
        self.log = open(log_file, "a")  # Append mode

    def write(self, message):
        if message.strip():  # Avoid logging empty lines
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.log.write(f"{timestamp} - {message}\n")
            self.log.flush()

    def flush(self):
        self.log.flush()

def setup_logger(log_file="logs/etl_log.log"):
    """
    Configures logging to write logs to a file and redirects sys.stdout.
    """
    logging.basicConfig(
        filename=log_file,
        filemode="a",  # Append logs
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Redirect stdout to LoggerWriter
    sys.stdout = LoggerWriter(log_file)
    return logging.getLogger()
