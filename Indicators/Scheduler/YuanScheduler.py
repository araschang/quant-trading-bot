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

def job_bitcoin_signal():
    indicator = YuanIndicator('BTC/USDT', 'binance', api_key, api_secret, 'YuanCopyTrade')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    signal = indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "BTC DETECT" DONE')
    return signal, ohlcv

def job_eth_signal():
    indicator = YuanIndicator('ETH/USDT', 'binance', api_key, api_secret, 'YuanCopyTrade')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    signal = indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "ETH DETECT" DONE')
    return signal, ohlcv

def job_trade(member_df):
    btc_signal, btc_ohlcv = job_bitcoin_signal()
    eth_signal, eth_ohlcv = job_eth_signal()
    for i in range(len(member_df)):
        api_key = member_df[i]['API_KEY']
        api_secret = member_df[i]['API_SECRET']
        exchange = member_df[i]['EXCHANGE']
        symbol = member_df[i]['SYMBOL']
        assetPercent = float(member_df[i]['ASSET_PERCENT'])
        stoplossPercent = float(member_df[i]['STOPLOSS_PERCENT'])
        strategy = member_df[i]['STRATEGY']

        indicator = YuanIndicator(symbol, exchange, api_key, api_secret, strategy)

        
        if symbol[:3] == 'BTC':
            signal = btc_signal
            now_price = indicator.getLivePrice()
            ohlcv = btc_ohlcv.copy()
        elif symbol[:3] == 'ETH':
            signal = eth_signal
            now_price = indicator.getLivePrice()
            ohlcv = eth_ohlcv.copy()
        try:
            indicator.openPosition(ohlcv, signal, assetPercent, 100, now_price, stoplossPercent)
        except Exception as e:
            logging.error('An error occurred: %s', e, exc_info=True)
            print(e)
        try:
            indicator.checkIfThereIsStopLoss(now_price, ohlcv)
        except Exception as e:
            logging.error('An error occurred: %s', e, exc_info=True)
            print(e)
        try:
            indicator.checkIfNoPositionCancelOpenOrder()
        except Exception as e:
            logging.error('An error occurred: %s', e, exc_info=True)
            print(e)
        
    print('JOB "TRADE" DONE')
    print('JOB "CHECK STOPLOSS" DONE')
    print('JOB "CHECK IF NO POSITION THEN CANCEL OPEN ORDER" DONE')

def job_trend_detect():
    indicator = YuanIndicator('BTC/USDT', 'binance', api_key, api_secret, 'YuanCopyTrade')
    indicator.checkTrend()
    print('JOB "TREND DETECT" DONE')

scheduler.add_job(job_trade, 'interval', seconds=3.8, args=[member])
# scheduler.add_job(job_trend_detect, 'interval', seconds=5, next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=3))
scheduler.start()