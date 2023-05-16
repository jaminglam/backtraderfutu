import logging
import backtrader as bt
import datetime

from datetime import timedelta
from backtraderfutu.futudata import FutuData
from strategies.teststrat import TestStrategy
from logging.config import fileConfig
from futu import *

fileConfig('logging.conf')
logger = logging.getLogger()


def runstrat():
    cerebro = bt.Cerebro()
    DataFactory = FutuData
    dataname0 = 'HK.00700'
    dataname1 = 'HK.01810'
    start_time = datetime.now() - timedelta(days=18)
    start_time = start_time.strftime('%Y-%m-%d')

    minutelydata0 = DataFactory(dataname=dataname0, start_time=start_time, timeframe=bt.TimeFrame.Minutes, compression=1, qcheck=2)
    logger.info('data0 feed created successfully: {}'.format(minutelydata0))
    fiveminutesdata0 = DataFactory(dataname=dataname0, start_time=start_time, timeframe=bt.TimeFrame.Minutes, compression=1, qcheck=2)
    logger.info('data0 feed created successfully: {}'.format(fiveminutesdata0))

    minutelydata1 = DataFactory(dataname=dataname1, start_time=start_time, timeframe=bt.TimeFrame.Minutes, compression=1, qcheck=2)
    logger.info('data1 feed created successfully: {}'.format(minutelydata1))
    fiveminutesdata1 = DataFactory(dataname=dataname1, start_time=start_time, timeframe=bt.TimeFrame.Minutes, compression=1, qcheck=2)
    logger.info('data1 feed created successfully: {}'.format(fiveminutesdata1))


    cerebro.adddata(minutelydata0, name=dataname0 + '_1m')
    # resample 1m data to 5m data
    cerebro.resampledata(fiveminutesdata0, name=dataname0 + '_5m', timeframe=bt.TimeFrame.Minutes, compression=5)
    cerebro.adddata(minutelydata1, name=dataname1 + '_1m')
    # resample 1m data to 5m data
    cerebro.resampledata(fiveminutesdata1, name=dataname1 + '_5m', timeframe=bt.TimeFrame.Minutes, compression=5)
    cerebro.broker.setcash(50000)
    cerebro.addstrategy(TestStrategy)
    results = cerebro.run()
    return cerebro

if __name__ == '__main__':
    runstrat()


    
