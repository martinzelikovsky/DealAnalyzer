import logging
import os
import sys



def config_logger(output_dir, logger):
    filename = os.path.join(output_dir, 'deal_analyzer.log')
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    fh = logging.FileHandler(filename)
    fh.setLevel = logging.DEBUG
    fh.setFormatter(format)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel = logging.INFO
    sh.setFormatter(format)
    logger.addHandler(fh)
    logger.addHandler(sh)