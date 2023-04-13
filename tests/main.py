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
    start_time = datetime.now() - timedelta(days=1)
    start_time = start_time.strftime('%Y-%m-%d')

    data0 = DataFactory(dataname=dataname0, start_time=start_time, trading_period=KLType.K_5M)
    logger.info('data0 feed created successfully: {}'.format(data0))
    data1 = DataFactory(dataname=dataname1, start_time=start_time, trading_period=KLType.K_5M)
    logger.info('data1 feed created successfully: {}'.format(data1))

    cerebro.adddata(data0, name=dataname0)
    cerebro.adddata(data1, name=dataname1)
    cerebro.broker.setcash(50000)
    cerebro.addstrategy(TestStrategy)
    results = cerebro.run()
    return cerebro

if __name__ == '__main__':
    runstrat()


    
