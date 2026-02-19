import logging
import sys
from pathlib import Path


def config_logger(output_dir: str, filename: str, logger: logging.Logger):
    log_file = Path(output_dir) / filename
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    fh = logging.FileHandler(log_file)
    fh.setLevel = logging.DEBUG
    fh.setFormatter(format)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel = logging.INFO
    sh.setFormatter(format)
    logger.addHandler(fh)
    logger.addHandler(sh)