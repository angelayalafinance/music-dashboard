# app/logging.py
from pythonjsonlogger import jsonlogger  # pip install python-json-logger
import logging
from logging.handlers import RotatingFileHandler
from utils.config import LOG_DIR
import os

def get_json_logger(name, log_level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    log_file_path = os.path.join(LOGS_DIR, f"{name}.log")

    handler = RotatingFileHandler(log_file_path, maxBytes=10*1024*1024)
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s',
        rename_fields={'levelname': 'severity', 'asctime': 'timestamp'}
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger
