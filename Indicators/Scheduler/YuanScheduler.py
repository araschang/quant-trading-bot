from apscheduler.schedulers.background import BackgroundScheduler
from Base.ConfigReader import Config
from Indicators.YuanIndicator import YuanIndicator
from Model.Service.MongoDBService import MongoDBService
import logging

logging.basicConfig(filename='quantlog.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
scheduler = BackgroundScheduler(job_defaults={'max_instances': 1})
config = Config()
api_key = config['Binance']['api_key']
api_secret = config['Binance']['api_secret']
mongo = MongoDBService()
query = {'STRATEGY': 'YuanCopyTrade'}
member = list(mongo._memberInfoConn().find(query))
symbol_lst = ['BTCUSDT', 'ETHUSDT']

def detect_signal(member):
    _livePriceConn = mongo._livePriceConn()
    _strategyConn = mongo._strategyConn()
    for i in range(len(symbol_lst)):
        livePrice = _livePriceConn.find_one({'SYMBOL': symbol_lst[i]}, sort=[('_id', -1)])
        time = livePrice['TIME']
        close = livePrice['CLOSE']
        volume = livePrice['VOLUME']
        strategy = _strategyConn.find_one({'SYMBOL': symbol_lst[i], 'STRATEGY': 'YuanCopyTrade'}, sort=[('_id', -1)])
        mean_vol = strategy['MEAN_VOLUME']
        atr = strategy['ATR']
        slope = strategy['SLOPE']
        if volume > mean_vol * 8:
            if slope <= 0:
                signal = 'buy'
            elif slope > 0:
                signal = 'sell'
            for i in range(len(member)):
                symbol = member[i]['SYMBOL']
                if symbol[:3] == symbol_lst[i][:3]:
                    api_key = member[i]['API_KEY']
                    api_secret = member[i]['API_SECRET']
                    exchange = member[i]['EXCHANGE']
                    assetPercent = float(member[i]['ASSET_PERCENT'])
                    strategy = member[i]['STRATEGY']
                try:
                    indicator = YuanIndicator(symbol, exchange, api_key, api_secret, strategy)
                    indicator.openPosition(signal, assetPercent, close, time, atr)
                except Exception as e:
                    logging.error(e)
                    print(e)
    print('DETECT SIGNAL IS DONE')

def detect_stoploss():
    pass

scheduler.add_job(detect_signal, 'interval', seconds=0.5, args=[member])
# scheduler.add_job(job_trend_detect, 'interval', seconds=5, next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=3))
scheduler.start()