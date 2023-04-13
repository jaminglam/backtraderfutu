from futu import *
from logging.config import fileConfig

import logging


# fileConfig('logging.conf')
logger = logging.getLogger(__name__)

############################ Global Variables ############################
FUTUOPEND_ADDRESS = '127.0.0.1'
FUTUOPEND_PORT = '11111'
TRADING_ENVIRONMENT = TrdEnv.SIMULATE  # Environment：REAL / SIMULATE


quote_context = None
trade_context = None


def open_context(host=FUTUOPEND_ADDRESS, port=FUTUOPEND_PORT):
    global quote_context, trade_context
    quote_context = OpenQuoteContext(
        host=host, port=port)
    trade_context = OpenHKTradeContext(host=host, port=port,
                                       security_firm=SecurityFirm.FUTUSECURITIES)  
    # quote_context.set_handler(OnBarClass)
    return quote_context, trade_context


def close_context():
    quote_context.close()
    trade_context.close()


def unlock_trade(trading_pwd, trade_context=trade_context, trading_env=TRADING_ENVIRONMENT):
    if trading_env == TrdEnv.REAL:
        ret, data = trade_context.unlock_trade(trading_pwd)
        if ret != RET_OK:
            logger.info('unlock trade failure：', data)
            return False
        logger.info('unlock trade successfully!')
    return True


def is_normal_trading_time(code):
    ret, data = quote_context.get_market_state([code])
    if ret != RET_OK:
        logger.info('retrieve market state failure：', data)
        return False
    market_state = data['market_state'][0]
    '''
    MarketState.MORNING            港、A 股早盘
    MarketState.AFTERNOON          港、A 股下午盘，美股全天
    MarketState.FUTURE_DAY_OPEN    港、新、日期货日市开盘
    MarketState.FUTURE_OPEN        美期货开盘
    MarketState.NIGHT_OPEN         港、新、日期货夜市开盘
    '''
    if market_state == MarketState.MORNING or \
            market_state == MarketState.AFTERNOON or \
            market_state == MarketState.FUTURE_DAY_OPEN or \
            market_state == MarketState.FUTURE_OPEN or \
            market_state == MarketState.NIGHT_OPEN:
        return True
    logger.info('market not in traidng hour')
    return False


def get_holding_position(code, trd_env=TRADING_ENVIRONMENT):
    holding_position = 0
    ret, data = trade_context.position_list_query(
        code=code, trd_env=trd_env)
    if ret != RET_OK:
        logger.info('retrieve holding position failure：', data)
        return None
    else:
        if data.shape[0] > 0:
            holding_position = data['qty'][0]
        logger.info('【holding status】 {} holding positions：{}'.format(code, holding_position))
    return holding_position



# 计算下单数量
def calculate_quantity(code):
    price_quantity = 0
    # 使用最小交易量
    ret, data = quote_context.get_market_snapshot([code])
    if ret != RET_OK:
        logger.info('获取快照失败：', data)
        return price_quantity
    price_quantity = data['lot_size'][0]
    return price_quantity
