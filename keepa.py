import argparse
import datetime
import pandas
import re
import keepa
import os
import logging

DOMAIN = 'CA'


def get_api():
    return keepa.Keepa(os.environ.get('KEEPA_KEY'))

def get_current_price():
    pass

def get_epoch_seconds_from_keepa_min(keepa_min):
    return (keepa_min + 21564000) * 60

def get_date_from_keepa_min(keepa_min):
    return datetime.date.fromtimestamp((keepa_min + 21564000) * 60)

def parse_args():
    args = argparse.ArgumentParser()


def main():
    parse_args()
    pass

if __name__ == '__main__':
    main()