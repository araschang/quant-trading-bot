from apscheduler.schedulers.background import BackgroundScheduler
from Base.ConfigReader import Config
from Indicators.YuanIndicator import YuanIndicator
from Model.Service.MongoDBService import MongoDBService
from datetime import datetime, timedelta
import logging


logging.basicConfig(filename='quantlog.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
scheduler = BackgroundScheduler(job_defaults={'max_instances': 1})
config = Config()
api_key = config['Binance']['api_key']
api_secret = config['Binance']['api_secret']
mongo = MongoDBService()
query = {'STRATEGY': 'YuanCopyTrade'}
member = list(mongo._memberInfoConn().find(query))
live_price_symbol_lst = ['BTCUSDT', 'ETHUSDT']
strategy_symbol_lst = ['BTC/USDT', 'ETH/USDT']

def detect_signal(member):
    _livePriceConn = mongo._livePriceConn()
    _strategyConn = mongo._strategyConn()
    for i in range(len(live_price_symbol_lst)):
        livePrice = _livePriceConn.find_one({'SYMBOL': live_price_symbol_lst[i]}, sort=[('_id', -1)])
        time = livePrice['TIME']
        close = livePrice['CLOSE']
        volume = livePrice['VOLUME']
        strategy = _strategyConn.find_one({'SYMBOL': strategy_symbol_lst[i], 'STRATEGY': 'YuanCopyTrade'}, sort=[('_id', -1)])
        mean_vol = strategy['MEAN_VOLUME']
        atr = strategy['ATR']
        slope = strategy['SLOPE']
        if volume > mean_vol * 8:
            if slope <= 0:
                signal = 'buy'
            elif slope > 0:
                signal = 'sell'
            for j in range(len(member)):
                symbol = member[j]['SYMBOL']
                if symbol[:3] == strategy_symbol_lst[i][:3]:
                    api_key = member[j]['API_KEY']
                    api_secret = member[j]['API_SECRET']
                    exchange = member[j]['EXCHANGE']
                    assetPercent = float(member[j]['ASSET_PERCENT'])
                    strategy = member[j]['STRATEGY']
                    try:
                        indicator = YuanIndicator(symbol, exchange, api_key, api_secret, strategy)
                        indicator.openPosition(signal, assetPercent, close, time, atr)
                    except Exception as e:
                        logging.error('An error occurred in YuanIndicator Detect Signal: %s', e, exc_info=True)
                        print(e)
    print('DETECT SIGNAL IS DONE')

def detect_stoploss(member):
    _transactionConn = mongo._transactionConn()
    _livePriceConn = mongo._livePriceConn()
    for i in range(len(member)):
        symbol = member[i]['SYMBOL']
        api_key = member[i]['API_KEY']
        api_secret = member[i]['API_SECRET']
        exchange = member[i]['EXCHANGE']
        strategy = member[i]['STRATEGY']
        position = list(_transactionConn.find({'API_KEY': api_key, 'SYMBOL': symbol, 'STRATEGY': strategy, 'IS_CLOSE': 0}, sort=[('_id', -1)]).limit(1))
        indicator = YuanIndicator(symbol, exchange, api_key, api_secret, strategy)
        has_position = len(position) > 0
        if has_position:
            try:
                live_price = _livePriceConn.find_one({'SYMBOL': symbol})['CLOSE']
                indicator.checkIfChangeStopLoss(live_price)
            except Exception as e:
                logging.error('An error occurred in YuanIndicator Detect Stop Loss: %s', e, exc_info=True)
                print(e)
        try:
            indicator.checkIfThereIsStopLoss()
        except Exception as e:
            logging.error('An error occurred in YuanIndicator Detect Stop Loss: %s', e, exc_info=True)
            print(e)
    print('DETECT STOPLOSS IS DONE')

scheduler.add_job(detect_signal, 'interval', seconds=0.5, args=[member], next_run_time=datetime.now() + timedelta(seconds=3))
scheduler.add_job(detect_stoploss, 'interval', seconds=1, args=[member], next_run_time=datetime.now() + timedelta(seconds=3))
# scheduler.add_job(job_trend_detect, 'interval', seconds=5, next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=3))
scheduler.start()