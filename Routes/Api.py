from flask_restful import Api
from flask import Flask
from discord import SyncWebhook
from apscheduler.schedulers.background import BackgroundScheduler
from Base.ConfigReader import Config
from Application.Api.Controller.WebsocketController import WebsocketController
from Application.Api.Controller.IndicatorController import IndicatorController
from Application.Indicators.YuanIndicator import YuanIndicator

app = Flask(__name__)
api = Api(app)
scheduler = BackgroundScheduler(job_defaults={'max_instances': 6})
config = Config()
stable_check_webhook = config['Discord']['stable_check']
aras_api_key = config['Binance_Aras']['api_key']
aras_api_secret = config['Binance_Aras']['api_secret']
yuan_api_key = config['Binance_Yuan']['api_key']
yuan_api_secret = config['Binance_Yuan']['api_secret']

def job_bitcoin_signal():
    indicator = YuanIndicator('BTC/USDT')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    signal = indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "BTC DETECT" DONE')

def job_eth_signal():
    indicator = YuanIndicator('ETH/USDT')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    signal = indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "ETH DETECT" DONE')

def job_btc_trade(api_key, api_secret, amount, stoploss):
    indicator = YuanIndicator('BTC/USDT')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    signal = indicator.checkSignal(mean_volume, ohlcv)
    now_price = float(ohlcv['close'].iloc[-1])
    indicator.openPosition(signal, amount, 100, now_price, stoploss, api_key, api_secret)
    print('JOB "BTC DETECT" DONE')

def job_eth_trade(api_key, api_secret, amount, stoploss):
    indicator = YuanIndicator('ETH/USDT')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    signal = indicator.checkSignal(mean_volume, ohlcv)
    now_price = float(ohlcv['close'].iloc[-1])
    indicator.openPosition(signal, amount, 100, now_price, stoploss, api_key, api_secret)
    print('JOB "ETH DETECT" DONE')

def job_trend_detect():
    indicator = YuanIndicator('BTC/USDT')
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

scheduler.add_job(job_bitcoin_signal, 'interval', seconds=5)
scheduler.add_job(job_eth_signal, 'interval', seconds=5)
# scheduler.add_job(job_eth_trade, 'interval', seconds=5, kwargs={'api_key': aras_api_key, 'api_secret': aras_api_secret, 'amount': 0.5, 'stoploss': 9})
# scheduler.add_job(job_eth_trade, 'interval', seconds=5, kwargs={'api_key': yuan_api_key, 'api_secret': yuan_api_secret, 'amount': 1, 'stoploss': 18})
scheduler.add_job(job_trend_detect, 'interval', seconds=5)
scheduler.add_job(stable_check, 'interval', hours=8)
scheduler.start()
