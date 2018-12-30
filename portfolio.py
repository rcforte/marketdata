import sys
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import time
import mysql.connector
import os.path
import datetime

from pandas_datareader import data
from lxml import html
from mysql.connector import errorcode
from sqlalchemy import create_engine
from marketdata import get_connection, get_engine

def calc_annualized_mean(daily_returns, weights):
    return np.sum(daily_returns.mean() * weights) * 252

def calc_annualized_vol(daily_returns, weights):
    cov_matrix = daily_returns.cov()

    portfolio_vol = np.dot(weights.T, np.dot(cov_matrix, weights))
    portfolio_vol = np.sqrt(portfolio_vol)
    portfolio_vol = portfolio_vol * np.sqrt(252)

    return portfolio_vol

if __name__ == "__main__":
    instruments = ['AAPL', 'MSFT', 'AMZN', 'GOOGL']
    weights = np.asarray([0.5, 0.2, 0.1, 0.2])

    daily_prices = get_instrument_prices(instruments)
    daily_returns = daily_prices.pct_change()
    daily_returns.to_csv('portfolio.csv')

    portfolio_return = calc_ann_mean(daily_returns, weights)
    print(portfolio_return)

    portfolio_vol = calc_ann_vol(daily_returns, weights)
    print(portfolio_vol)

