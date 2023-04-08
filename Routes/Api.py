from flask_restful import Api
from flask import Flask
from discord import SyncWebhook
import datetime
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from Base.ConfigReader import Config
from Application.Api.Controller.WebsocketController import WebsocketController
from Application.Api.Controller.IndicatorController import IndicatorController
from Application.Indicators.YuanIndicator import YuanIndicator
from Application.Api.Service.WebsocketService import WebsocketService
from Application.Api.Service.MongoDBService import MongoDBService

app = Flask(__name__)
api = Api(app)
scheduler = BackgroundScheduler(job_defaults={'max_instances': 1})
config = Config()
logging.basicConfig(filename='quantlog.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
stable_check_webhook = config['Discord']['stable_check']
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
    # eth_signal, eth_ohlcv = job_eth_signal()
    for i in range(len(member_df)):
        api_key = member_df[i]['API_KEY']
        api_secret = member_df[i]['API_SECRET']
        exchange = member_df[i]['EXCHANGE']
        symbol = member_df[i]['SYMBOL']
        assetPercent = float(member_df[i]['ASSET_PERCENT'])
        stoplossPercent = float(member_df[i]['STOPLOSS_PERCENT'])
        strategy = member_df[i]['STRATEGY']

        indicator = YuanIndicator(symbol, exchange, api_key, api_secret, strategy)

        try:
            if symbol[:3] == 'BTC':
                signal = btc_signal
                now_price = indicator.getLivePrice()
                ohlcv = btc_ohlcv.copy()
            # elif symbol[:3] == 'ETH':
            #     signal = eth_signal
            #     now_price = indicator.getLivePrice()
            #     ohlcv = eth_ohlcv.copy()
            indicator.openPosition(ohlcv, signal, assetPercent, 100, now_price, stoplossPercent)
            indicator.checkIfThereIsStopLoss(now_price, ohlcv)
            indicator.checkIfNoPositionCancelOpenOrder()
        except Exception as e:
            logging.error('An error occurred: %s', e, exc_info=True)
            print(e)
    print('JOB "CHECK STOPLOSS" DONE')
    print('JOB "TRADE" DONE')
    print('JOB "CHECK IF NO POSITION THEN CANCEL OPEN ORDER" DONE')

def binance_websocket():
    try:
        websocket = WebsocketService()
        websocket.binanceWebsocket('btcusdt', '3m')
    except Exception as e:
        logging.error('An error occurred: %s', e, exc_info=True)
        print(e)

def job_trend_detect():
    indicator = YuanIndicator('BTC/USDT', 'binance', api_key, api_secret, 'Yuan')
    indicator.checkTrend()
    print('JOB "TREND DETECT" DONE')

def stable_check():
    webhook = SyncWebhook.from_url(stable_check_webhook)
    webhook.send('STABLE CHECK')

api.add_resource(
    WebsocketController,
    '/api/websocket',
)

api.add_resource(
    IndicatorController,
    '/api/indicator',
)

# scheduler.add_job(job_bitcoin_signal, 'interval', seconds=5)
# scheduler.add_job(job_eth_signal, 'interval', seconds=5)
scheduler.add_job(job_trade, 'interval', seconds=3.5, args=[member])
# scheduler.add_job(check_stoploss_order, 'interval', seconds=5, next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=2))
# scheduler.add_job(job_trend_detect, 'interval', seconds=5, next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=3))
# scheduler.add_job(job_check_if_no_position_then_cancel_open_order, 'interval', seconds=3, next_run_time=scheduler.get_jobs()[0].next_run_time)
scheduler.add_job(binance_websocket, 'interval', hours=24, next_run_time=datetime.datetime.now(), max_instances=2)
# scheduler.add_job(stable_check, 'interval', hours=8)
scheduler.start()




