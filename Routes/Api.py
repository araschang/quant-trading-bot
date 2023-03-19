from flask_restful import Api
from flask import Flask
from discord import SyncWebhook
import pandas as pd
import os
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
api_key = config['Binance']['api_key']
api_secret = config['Binance']['api_secret']

def job_bitcoin_signal():
    indicator = YuanIndicator('BTC/USDT', 'binance', api_key, api_secret)
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "BTC DETECT" DONE')

def job_eth_signal():
    indicator = YuanIndicator('ETH/USDT', 'binance', api_key, api_secret)
    ohlcv = indicator.getOHLCV('3m')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "ETH DETECT" DONE')

def job_trade():
    member_df = pd.read_csv(os.path.join(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Application/Indicators', 'YuanMember.csv')))
    for i in range(len(member_df)):
        api_key = member_df['API_KEY'].iloc[i]
        api_secret = member_df['API_SECRET'].iloc[i]
        exchange = member_df['EXCHANGE'].iloc[i]
        symbol = member_df['SYMBOL'].iloc[i]
        amount = float(member_df['AMOUNT'].iloc[i])
        stoploss = float(member_df['STOPLOSS'].iloc[i])

        indicator = YuanIndicator(symbol, exchange, api_key, api_secret)
        ohlcv = indicator.getOHLCV('3m')
        mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
        signal = indicator.checkSignal(mean_volume, ohlcv)
        now_price = float(ohlcv['close'].iloc[-1])
        indicator.openPosition(signal, amount, 100, now_price, stoploss)
    print('JOB "TRADE" DONE')

def check_stoploss_order():
    member_df = pd.read_csv(os.path.join(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Application/Indicators', 'YuanMember.csv')))
    for i in range(len(member_df)):
        api_key = member_df['API_KEY'].iloc[i]
        api_secret = member_df['API_SECRET'].iloc[i]
        exchange = member_df['EXCHANGE'].iloc[i]
        symbol = member_df['SYMBOL'].iloc[i]
        indicator = YuanIndicator(symbol, exchange, api_key, api_secret)
        indicator.checkIfThereIsStopLoss()
    print('JOB "CHECK STOPLOSS" DONE')

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
scheduler.add_job(job_trade, 'interval', seconds=5)
scheduler.add_job(check_stoploss_order, 'interval', seconds=5)
scheduler.add_job(stable_check, 'interval', hours=8)
scheduler.start()

if __name__ == '__main__':
    print(os.path.join(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Application/Indicators', 'YuanMember.csv')))

