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

def job_bitcoin_trade():
    indicator = YuanIndicator('BTC/USDT')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "BITCOIN TRADE" DONE')

def job_eth_trade():
    indicator = YuanIndicator('ETH/USDT')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "ETH TRADE" DONE')

def job_sol_trade():
    indicator = YuanIndicator('SOL/USDT')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "SOL TRADE" DONE')

def job_bnb_trade():
    indicator = YuanIndicator('BNB/USDT')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "BNB TRADE" DONE')

def job_xrp_trade():
    indicator = YuanIndicator('XRP/USDT')
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "XRP TRADE" DONE')

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

scheduler.add_job(job_bitcoin_trade, 'interval', seconds=30)
scheduler.add_job(job_eth_trade, 'interval', seconds=30)
scheduler.add_job(job_sol_trade, 'interval', seconds=30)
scheduler.add_job(job_bnb_trade, 'interval', seconds=30)
scheduler.add_job(job_xrp_trade, 'interval', seconds=30)
scheduler.add_job(stable_check, 'interval', hours=8)
scheduler.start()
