import threading
import collections
import time
import backtrader as bt

from backtraderfutu import futu_util
from datetime import datetime
from datetime import timedelta
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import bytes, bstr, queue, with_metaclass, long
from backtrader import TimeFrame, Position
from datetime import date, datetime, timedelta
from backtrader.utils import AutoDict, UTC
from futu import *

logger = logging.getLogger()


class Streamer():
    last_time = None

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.quote_context = kwargs['quote_context'] 
        self.dataname = kwargs['dataname']
        self.trading_period = kwargs['trading_period']
        self.q = kwargs.get('q', None)
        self.qhist = kwargs.get('qhist', None)
        self.sleep_sec = kwargs.get('sleep_sec', 1)

    def connected(self):
        if (not futu_util.trade_context) or (not futu_util.quote_context):
            return False
        if futu_util.quote_context.status != ContextStatus.READY or futu_util.trade_context.status != ContextStatus.READY: 
            return False
        return True

    def generate_endtime(self):
        # minutes_level_kltypes = [KLType.K_1M, KLType.K_3M, KLType.K_5M, KLType.K_15M, KLType.K_30M, KLType.K_60M]
        # if self.trading_period == KLType.K_DAY:
        #     end_time = datetime.now() - timedelta(days=1) 
        #     # end_time = datetime.now()
        # elif self.trading_period in minutes_level_kltypes:
        #     end_time = datetime.now()
        # else:
        #     # TODO
        #     end_time = datetime.now()
        end_time = datetime.now()
        end_time = end_time.strftime('%Y-%m-%d')
        return end_time

    def put_histdata(self, data):
        # need to reverse data make sure with time descending order
        # reversed_data = data[::-1] 
        logger.debug('historical_data:')
        logger.debug(data)
        logger.info('try to put code %s %s (%d) data in hist queue %s' % (self.dataname, self.trading_period, len(data), self.qhist))
        for index, row in data.iterrows():
            subdata = data.iloc[[index]].reset_index()
            self.qhist.put({'data': subdata})

    def stream_history(self, start_time):
        try:
            if self.qhist is None:
                logger.error('qhist is None')
                return
            end_time = self.generate_endtime()  
            ret, data, page_req_key = self.quote_context.request_history_kline(
                self.dataname, start=start_time, end=end_time, 
                max_count=1000, ktype=self.trading_period, 
                autype=AuType.NONE)
            if ret == RET_OK:
                self.put_histdata(data)
                logger.info('put code %s %s data in hist queue %s successfully' % (self.dataname, self.trading_period, self.qhist))
            else:
                self.qhist.put({'code': ret, 'data': data})
            while page_req_key != None:
                ret, data, page_req_key = self.quote_context.request_history_kline(
                    self.dataname, start=start_time, end=end_time, 
                    max_count=1000, ktype=self.trading_period, 
                    autype=AuType.NONE, page_req_key=page_req_key)
                if ret == RET_OK:
                    self.put_histdata(data)
                    logger.info('put code %s %s data in hist queue %s successfully' % (self.dataname, self.trading_period, self.qhist))
                else:
                    self.qhist.put({'code': ret, 'data': data})
                    raise Exception('continuous stream historical data failed')
            self.qhist.put({})
        except Exception as e:
            logger.error('stream historical klines failed. msg: %s' % e)
            logger.error(e)
            self.qhist.put(None)
            return

    def stream(self):
        if self.q is None:
            logger.error('q is None')
            return
        while self.connected():
            try:
                time.sleep(self.sleep_sec)
                logger.debug('quote context %s status after sleep: %s' % (self.quote_context, self.quote_context.status))
                ret, data = self.quote_context.get_cur_kline(self.dataname, 1, self.trading_period, autype=AuType.NONE)
                logger.debug(data)
                if ret == RET_OK:
                    cur_time = data['time_key'][0]
                    if cur_time != self.last_time: 
                        logger.info('try to put code %s data in queue %s' % (self.dataname, self.q))
                        # logger.info(data)
                        msg = {'data': data}    
                        self.q.put(msg)
                        logger.info('put code %s data in queue %s successfully' % (self.dataname, self.q))
                        self.last_time = cur_time
                else:
                    logger.error('streaming %s klines failed. msg: %s' %  (self.dataname, data))
                    msg = {'code': ret, 'data': data}
                    logger.error(msg)
                    logger.error('quote context: %s, status: %s' % (self.quote_context, self.quote_context.status))
            except Exception as e:
                    logger.info('streaming %s klines failed. msg: %s' % (self.dataname, e))
                    logger.info(e)
        return

class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton

class FutuStore(with_metaclass(MetaSingleton, object)):
    params = (
        ('host', '127.0.0.1'),
        ('port', 11111),
        ('trading_password', ''),
        ('notifyall', False),
        ('_debug', False),
        ('reconnect', 3),  # -1 forever, 0 No, > 0 number of retries
        ('timeout', 3.0),  # timeout between reconnections
    )

    _GRANULARITIES = {
        (bt.TimeFrame.Minutes, 1): KLType.K_1M,
        (bt.TimeFrame.Minutes, 5): KLType.K_5M,
        (bt.TimeFrame.Minutes, 15): KLType.K_15M,
        (bt.TimeFrame.Minutes, 30): KLType.K_30M,
        (bt.TimeFrame.Minutes, 60): KLType.K_60M,
        (bt.TimeFrame.Days, 1): KLType.K_DAY,
        (bt.TimeFrame.Months, 1): KLType.K_MON,
        (bt.TimeFrame.Years, 1): KLType.K_YEAR
    }

    _TMOUTS = {
        KLType.K_1M: 1,
        KLType.K_5M: 1,
        KLType.K_15M: 2,
        KLType.K_30M: 2,
        KLType.K_60M: 2,
        KLType.K_DAY: 10,
        KLType.K_MON: 10,
        KLType.K_YEAR: 10
    }

    DataCls = None
    @classmethod
    def getdata(cls, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(FutuStore, self).__init__()

        self.dontreconnect = False  # for non-recoverable connect errors
        self.trading_password = self.p.trading_password
        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start
        self.ccount = 0  # requests to start (from cerebro or datas)
        self.positions = collections.defaultdict(Position)  # actual positions
        self.orderid = None  # next possible orderid (will be itertools.count)
        self.notifs = queue.Queue()  # store notifications for cerebro

        self.quote_context = None
        self.trade_context = None

        self._cash = 0.0
        self._value = 0.0

    # TODO: This is a temp solution and neet to be optimized
    # function to calculate sleep time for polling live candles
    def _get_tmout(self, granularity):
        return self._TMOUTS(granularity)        
    
    def get_granularity(self, timeframe, compression):
        return self._GRANULARITIES.get((timeframe, compression), None)

    def start(self, data=None, broker=None):
        # self.reconnect()
        if data is None and broker is None:
            self.cash = None
            return
        # Datas require some processing to kickstart data reception
        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            # if connection fails, get a fake registration that will force the
            # datas to try to reconnect or else bail out
            # return self.getTickerQueue(start=True)

        elif broker is not None:
            self.broker = broker
            

    def connected(self):
        if (not futu_util.trade_context) or (not futu_util.quote_context):
            return False
        if futu_util.quote_context.status != ContextStatus.READY or futu_util.trade_context.status != ContextStatus.READY: 
            return False
        return True

    def reconnect(self):
        first_connect = False
        if self.connected():
            # connected, nothing to do 
            return True
        retries = self.p.reconnect
        retries += 1
        while retries > 0:
            try:
                if not first_connect:
                    time.sleep(self.p.timeout)
                else:
                    first_connect = False
                # TODO
                # self.quote_context, self.trade_context = futu_util.open_context(host=self.p.host, port=self.p.port)
                return True
            except:
                retries -= 1
        return False

    def stop(self):
        if self.connected():
            futu_util.close_context()
        return True

    def subscribe_klines(self, dataname, timeframe, compression):
        granularity = self.get_granularity(timeframe, compression)
        logger.info('try to subscribe %s klines' % dataname)
        logger.info('try to open context, host=%s, port=%s' % (self.p.host, self.p.port))
        self.quote_context, self.trade_context = futu_util.open_context(host=self.p.host, port=self.p.port)
        logger.info('data %s subscribe quote context: %s, trade context: %s' % (dataname, self.quote_context, self.trade_context))
        try:
            logger.info('current subscription status :{}'.format(self.quote_context.query_subscription()))
            # self.quote_context.subscribe(code_list=[dataname], subtype_list=[SubType.TICKER, SubType.ORDER_BOOK, KLType.K_1M])
            self.quote_context.subscribe(code_list=[dataname], subtype_list=[SubType.TICKER, SubType.ORDER_BOOK, granularity])
            logger.info('subscribe successfully! current subscription status: {}'.format(self.quote_context.query_subscription()))
        except Exception as e:
            self._state = self._ST_OVER
            logger.info("subscribe failed. msg: %s" % e)
            return
 
    def _t_streaming_historical_klines(self, qhist, dataname, start_time, granularity):
        streamer = Streamer(quote_context=self.quote_context,
                            dataname=dataname, trading_period=granularity,
                            qhist=qhist)
        streamer.stream_history(start_time=start_time) 

    def streaming_historical_klines(self, dataname, start_time, timeframe, compression):
        q = queue.Queue()
        granularity = self.get_granularity(timeframe, compression)
        logger.info('create queue %s for stream historical klines' % q)
        kwargs = {'qhist': q, 'dataname': dataname, 'start_time': start_time, 'granularity': granularity}
        logger.info('kwargs = {}'.format(kwargs))
        t = threading.Thread(target=self._t_streaming_historical_klines, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def _t_streaming_klines(self, q, dataname, granularity, tmout):
        streamer = Streamer(quote_context=self.quote_context, 
                            dataname=dataname, trading_period=granularity,
                            q=q, sleep_sec=tmout)
        streamer.stream()

    def streaming_klines(self, dataname, timeframe, compression):
        granularity = self.get_granularity(timeframe, compression)
        tmout = self._get_tmout(granularity)
        q = queue.Queue()
        logger.info('create queue %s for stream klines' % q)
        kwargs = {'q': q, 'dataname': dataname, 'granularity': granularity, 'tmout': tmout}
        logger.info('kwargs = {}'.format(kwargs))
        t = threading.Thread(target=self._t_streaming_klines, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q
       
    def get_cash(self):
        return self._cash
    
    def get_value(self):
        return self._value
    

