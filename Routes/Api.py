from flask_restful import Api
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from Application.Api.Controller.WebsocketController import WebsocketController
from Application.Api.Controller.IndicatorController import IndicatorController
from Application.Indicators.YuanIndicator import YuanIndicator

app = Flask(__name__)
api = Api(app)
scheduler = BackgroundScheduler(job_defaults={'max_instances': 2})

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
scheduler.start()
