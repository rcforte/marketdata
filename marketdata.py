#!/usr/bin/env python

from sqlalchemy import create_engine

import sys
import requests
import pandas as pd
import matplotlib.pyplot as plt
import logging
import time
import mysql.connector
import os.path
import datetime

from pandas_datareader import data
from lxml import html
from mysql.connector import errorcode

class Instrument(object):
    def __init__(self, symbol, name, gics_sector, gics_subindustry):
        self.instrument_id = None
        self.symbol = symbol
        self.name = name
        self.gics_sector = gics_sector
        self.gics_subindustry = gics_subindustry

    def __str__(self):
        return '[Instrument: symbol=%s, name=%s, gics_sector=%s, gics_subindustry=%s]' \
                % (self.symbol, self.name, self.gics_sector, self.gics_subindustry)

def _download_instruments():
    logging.info('Loading SP500 components from wikipedia...')
    wikipedia_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    page = requests.get(wikipedia_url)
    root = html.fromstring(page.content)

    # Look for the table containing the index components
    tables = root.xpath("//table[@class='wikitable sortable']")
    table = tables[0]
    first_row = table.xpath('//tr[1]')[0]

    # Get the header
    header = [th.text_content() for th in first_row.xpath('./th')]
    print('Header: ', header)

    # Get the instruments
    instruments = []
    for row_index, row in enumerate(table.xpath('//tr')):
        # First row is the header
        if row_index == 0:
            continue

        cells = row.xpath('./td')
        print('Cells:', cells)

        instrument = Instrument(
                cells[0].text_content(),
                cells[1].text_content(),
                cells[3].text_content(),
                cells[4].text_content())
        instruments.append(instrument)

    # Sort by symbol
    instruments.sort(key=lambda instrument: instrument.symbol)
    return instruments

def download_instruments():
    data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    table = data[0]
    ret = [Instrument(row[0], row[1], row[3], row[4]) for _, row in table.iterrows()]
    ret.sort(key=lambda x: x.symbol)
    return ret

def get_instruments(symbols):
    cnx = get_connection()

    criteria = ["'{}'".format(symbol) for symbol in symbols]
    criteria = ','.join(criteria)
    query = (
            'select instrument_id, instrument_symbol, instrument_name, gics_sector, gics_subindustry '
            'from tbl_instrument '
            'where instrument_symbol in ({})'.format(criteria)
            )

    instruments = []
    try:
        cursor = cnx.cursor()
        cursor.execute(query)
        for (instrument_id, symbol, name, gics_sector, gics_subindustry) in cursor:
            instr = Instrument(symbol, name, gics_sector, gics_subindustry)
            instr.instrument_id = instrument_id
            instruments.append(instr)
        cnx.commit()
    except mysql.connector.Error as err:
        if cnx:
            cnx.rollback()
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error('Something wrong with your username or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error('Database does not exist')
        else:
            logging.error(err)
    else:
        cursor.close()
        cnx.close()

    return instruments


def get_instrument_prices(symbols):
    engine = get_engine()

    prices = None
    instruments = get_instruments(symbols)
    for instrument in instruments:
        query = '''
            select price_date, adj_close
            from tbl_instrument_price
            where instrument_id = {}
        '''.format(instrument_id)
        price = pd.read_sql(query, con=engine, index_col='price_date')
        price.columns = [instrument_symbol]

        if prices is None:
            prices = price
        else:
            prices = prices.join(price, how='inner')

    return prices


def save_instruments(instruments):
    logging.info('saving instruments')
    cnx = get_connection()

    try:
        cursor = cnx.cursor(buffered=True)
        engine = get_engine()

        insert = (
                'insert into tbl_instrument '
                '   (instrument_symbol, instrument_name, gics_sector, gics_subindustry) '
                'values '
                '   (%s, %s, %s, %s)'
                )
        for instrument in instruments:
            # Get instrument id
            query = (
                'select instrument_id '
                'from tbl_instrument '
                'where instrument_symbol = %s'
            )
            query_data = (str(instrument.symbol), )
            cursor.execute(query, query_data)
            if cursor.rowcount:
                instrument.instrument_id = cursor.fetchone()[0]
                continue

            # If instrument id does not exist, insert it
            logging.info('inserting instrument %s', instrument.symbol)
            insert_data = (
                str(instrument.symbol),
                str(instrument.name),
                str(instrument.gics_sector),
                str(instrument.gics_subindustry)
            )
            cursor.execute(insert, insert_data)

            # Save database auto generated id
            instrument.instrument_id = cursor.lastrowid

        cnx.commit()
    except mysql.connector.Error as err:
        if cnx:
            cnx.rollback()
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error('Something wrong with your username or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error('Database does not exist')
        else:
            logging.error('There was a general error: %s, %s', err, err.with_traceback)
    else:
        cursor.close()
        cnx.close()


def download_prices(instruments, output_folder, start_date, end_date, data_source='yahoo'):
    for instrument in instruments:
        download_price(instrument, output_folder, start_date, end_date, data_source)

def download_price(instrument, output_folder, start_date, end_date, data_source):
    try:
        file_name = '{}.csv'.format(instrument.symbol)
        file_name = os.path.join(folder, file_name)
        instrument.file_name = file_name

        if os.path.exists(file_name):
            return

        logging.info('Downloading prices for %s...', instrument.symbol)
        panel_data = data.DataReader(instrument.symbol, data_source, start_date)
        panel_data.to_csv(file_name)
    except Exception as e:
        logging.error('Failed to download %s: %s', instrument.symbol, str(e))

        # If we could not save the stock prices in a csv file, mark
        # the file name with an invalid value
        instrument.file_name = None

def get_connection():
    return mysql.connector.connect(
        user='market_data_user',
        password='market_data_user',
        host='127.0.0.1',
        database='db_market_data')

def get_engine():
    return create_engine('mysql+mysqlconnector://market_data_user:market_data_user@localhost:3306/db_market_data')

def save_instruments_prices(instruments):
    cnx = get_connection()
    try:
        # First, make sure we wipe out the content of the table
        cursor = cnx.cursor()
        cursor.execute('truncate table tbl_instrument_price')
        engine = get_engine()

        # For each instrument being saved
        for instrument in instruments:
            # Check if the instrument name is valid
            if instrument.file_name:
                save_instrument_price(instrument)
        cnx.commit()
    except mysql.connector.Error as err:
        if cnx:
            cnx.rollback()
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error('Something wrong with your username or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error('Database does not exist')
        else:
            logging.error(err)
    else:
        cursor.close()
        cnx.close()

def save_instrument_price(instrument):
    engine = get_engine()

    # Insert instrument prices using pandas
    logging.info('Inserting prices for %s', instrument.symbol)
    prices_data = pd.read_csv(instrument.file_name)
    prices_data.columns = [
        'price_date',
        'open',
        'high',
        'low',
        'close',
        'adj_close',
        'volume']
    prices_data['instrument_id'] = instrument.instrument_id
    prices_data.to_sql(
        name='tbl_instrument_price',
        con=engine,
        if_exists='append',
        index=False,
        chunksize=1000)

if __name__ == '__main__':
    # create instruments based on the SP500
    logging.basicConfig(level=logging.INFO)
    instruments = download_instruments()
    save_instruments(instruments)

    # create folder with current date
    current_date = datetime.datetime.now()
    folder = current_date.strftime('%Y-%m-%d')
    os.makedirs(folder, exist_ok=True)

    start_date = '2000-01-01'
    end_date = '2018-01-01'
    download_prices(instruments, folder, start_date, end_date)
    save_instruments_prices(instruments)

