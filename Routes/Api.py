from flask_restful import Api
from flask import Flask
from discord import SyncWebhook
import pandas as pd
import os
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from Base.ConfigReader import Config
from Application.Api.Controller.WebsocketController import WebsocketController
from Application.Api.Controller.IndicatorController import IndicatorController
from Application.Indicators.YuanIndicator import YuanIndicator

app = Flask(__name__)
api = Api(app)
scheduler = BackgroundScheduler(job_defaults={'max_instances': 7})
config = Config()
stable_check_webhook = config['Discord']['stable_check']
api_key = config['Binance']['api_key']
api_secret = config['Binance']['api_secret']

def job_bitcoin_signal():
    indicator = YuanIndicator('BTC/USDT', 'binance', api_key, api_secret, 'Yuan')
    ohlcv = indicator.getOHLCV('3m')
    ohlcv.to_csv('BTCUSDT_now.csv')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    signal = indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "BTC DETECT" DONE')
    return signal

def job_eth_signal():
    indicator = YuanIndicator('ETH/USDT', 'binance', api_key, api_secret, 'Yuan')
    ohlcv = indicator.getOHLCV('3m')
    ohlcv.to_csv('ETHUSDT_now.csv')
    mean_volume = indicator.cleanData2GenerateMeanVolume(ohlcv)
    signal = indicator.checkSignal(mean_volume, ohlcv)
    print('JOB "ETH DETECT" DONE')
    return signal

def job_trade():
    member_df = pd.read_csv(os.path.join(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Application/Indicators', 'YuanMember.csv')))
    btc_signal = job_bitcoin_signal()
    eth_signal = job_eth_signal()
    for i in range(len(member_df)):
        api_key = member_df['API_KEY'].iloc[i]
        api_secret = member_df['API_SECRET'].iloc[i]
        exchange = member_df['EXCHANGE'].iloc[i]
        symbol = member_df['SYMBOL'].iloc[i]
        assetPercent = float(member_df['ASSET_PERCENT'].iloc[i])
        stoplossPercent = float(member_df['STOPLOSS_PERCENT'].iloc[i])

        indicator = YuanIndicator(symbol, exchange, api_key, api_secret, 'Yuan')

        if exchange == 'binance':
            symbol = symbol.split('/')
            symbol = symbol[0] + symbol[1]

        ohlcv = pd.read_csv(symbol + '_now.csv')
        now_price = float(ohlcv['close'].iloc[-1])
        if symbol[:3] == 'BTC':
            signal = btc_signal
        elif symbol[:3] == 'ETH':
            signal = eth_signal
        indicator.openPosition(signal, assetPercent, 100, now_price, stoplossPercent)
        indicator.checkIfThereIsStopLoss(now_price)
    print('JOB "CHECK STOPLOSS" DONE')
    print('JOB "TRADE" DONE')

    member_df = pd.read_csv(os.path.join(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Application/Indicators', 'YuanMemberForCopyTrade.csv')))
    for i in range(len(member_df)):
        api_key = member_df['API_KEY'].iloc[i]
        api_secret = member_df['API_SECRET'].iloc[i]
        exchange = member_df['EXCHANGE'].iloc[i]
        symbol = member_df['SYMBOL'].iloc[i]
        assetPercent = float(member_df['ASSET_PERCENT'].iloc[i])
        stoplossPercent = float(member_df['STOPLOSS_PERCENT'].iloc[i])

        indicator = YuanIndicator(symbol, exchange, api_key, api_secret, 'YuanCopyTrade')

        if exchange == 'binance':
            symbol = symbol.split('/')
            symbol = symbol[0] + symbol[1]
        ohlcv = pd.read_csv(symbol + '_now.csv')
        now_price = float(ohlcv['close'].iloc[-1])
        if symbol[:3] == 'BTC':
            signal = btc_signal
        elif symbol[:3] == 'ETH':
            signal = eth_signal
        indicator.openPosition(signal, assetPercent, 100, now_price, stoplossPercent)
        indicator.checkIfThereIsStopLossForCopyTrade(now_price)
    print('JOB "COPY TRADE" DONE')


def check_stoploss_order():
    member_df = pd.read_csv(os.path.join(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Application/Indicators', 'YuanMember.csv')))
    for i in range(len(member_df)):
        api_key = member_df['API_KEY'].iloc[i]
        api_secret = member_df['API_SECRET'].iloc[i]
        exchange = member_df['EXCHANGE'].iloc[i]
        symbol = member_df['SYMBOL'].iloc[i]
        indicator = YuanIndicator(symbol, exchange, api_key, api_secret)
        now_price = float(indicator.getOHLCV('3m')['close'].iloc[-1])
        indicator.checkIfThereIsStopLoss(now_price)
    print('JOB "CHECK STOPLOSS" DONE')

def job_trend_detect():
    indicator = YuanIndicator('BTC/USDT', 'binance', api_key, api_secret, 'Yuan')
    indicator.checkTrend()
    print('JOB "TREND DETECT" DONE')

def job_check_if_no_position_then_cancel_open_order(): # for bybit
    member_df = pd.read_csv(os.path.join(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Application/Indicators', 'YuanMember.csv')))
    for i in range(len(member_df)):
        api_key = member_df['API_KEY'].iloc[i]
        api_secret = member_df['API_SECRET'].iloc[i]
        exchange = member_df['EXCHANGE'].iloc[i]
        symbol = member_df['SYMBOL'].iloc[i]
        indicator = YuanIndicator(symbol, exchange, api_key, api_secret)
        indicator.checkIfNoPositionCancelOpenOrder()
    print('JOB "CHECK IF NO POSITION THEN CANCEL OPEN ORDER" DONE')

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
scheduler.add_job(job_trade, 'interval', seconds=5, next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=1))
# scheduler.add_job(check_stoploss_order, 'interval', seconds=5, next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=2))
scheduler.add_job(job_trend_detect, 'interval', seconds=5, next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=3))
# scheduler.add_job(job_check_if_no_position_then_cancel_open_order, 'interval', seconds=5, next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=4))
scheduler.add_job(stable_check, 'interval', hours=8)
scheduler.start()

if __name__ == '__main__':
    print(os.path.join(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Application/Indicators', 'YuanMember.csv')))

