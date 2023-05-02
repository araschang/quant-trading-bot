from apscheduler.schedulers.background import BackgroundScheduler
from Model.Service.WebsocketService import WebsocketService
from Base.Service.DiscordService import DiscordService
from Base.ConfigReader import Config
from datetime import datetime, timedelta
from Model.Service.MongoDBService import MongoDBService
from Model.Service.StrategyService import StrategyService
import logging
import ccxt
import time

logging.basicConfig(filename='quantlog.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
scheduler = BackgroundScheduler(job_defaults={'max_instances': 3})
strategy = StrategyService()
config = Config()
aras_api_key = config['Binance_Aras']['api_key']
aras_api_secret = config['Binance_Aras']['api_secret']
yuan_api_key = config['Binance_Yuan']['api_key']

def binance_btc_websocket():
    websocket = WebsocketService()
    websocket.binancePriceWebsocket('btcusdt', '3m')

def binance_eth_websocket():
    websocket = WebsocketService()
    websocket.binancePriceWebsocket('ethusdt', '3m')

def binance_all_market_websocket():
    websocket = WebsocketService()
    websocket.binanceAllMarketWebsocket()

def aras_account_websocket():
    print('aras_account_websocket')
    websocket = WebsocketService()
    websocket.binanceAccountWebsocket(aras_api_key)

def yuan_account_websocket():
    print('yuan_account_websocket')
    websocket = WebsocketService()
    websocket.binanceAccountWebsocket(yuan_api_key)

def YuanIndicatorSignal():
    try:
        strategy = StrategyService()
        strategy.YuanIndicatorGenerator()
    except Exception as e:
        logging.error('An error occurred in YuanIndicatorGenerator: %s', e, exc_info=True)
        print(e)
    print('YuanIndicatorGenerator DONE')

def leverage_updater():
    exchange = ccxt.binanceusdm({
        'apiKey': aras_api_key,
        'secret': aras_api_secret,
        'enableRateLimit': True,
        'option': {
            'defaultMarket': 'future',
        },
    })
    mongo = MongoDBService()
    market = exchange.fetch_markets()
    for i in range(len(market)):
        if market[i]['id'][-4:] != 'USDT':
            continue
        leverage = exchange.fetch_leverage_tiers([market[i]['id']])
        max_leverage = leverage[market[i]['id'][:-4] + '/' + market[i]['id'][-4:] + ':USDT'][0]['maxLeverage']
        query = {'SYMBOL': market[i]['id'], 'EXCHANGE': 'BINANCE', 'MAX_LEVERAGE': int(max_leverage)}
        mongo._leverageConn().update_one({'SYMBOL': market[i]['id'], 'EXCHANGE': 'BINANCE'}, {'$set': query}, upsert=True)
        print(f'{market[i]["id"]} leverage updated to {max_leverage}')
        time.sleep(1)

def error_handler(job_id, exception):
    print(f'Error in job {job_id}: {exception}')
    logging.error('An error occurred in %s: %s', job_id, exception, exc_info=True)
    scheduler.add_job(safe_run, 'date', id=job_id, run_date=datetime.now(), args=[job_id])

def safe_run(job_id):
    try:
        globals()[job_id]()
    except Exception as e:
        error_handler(job_id, e)

def stable_check():
    db_check()
    webhook = DiscordService()
    webhook.stableCheck('Staging: All jobs are running normally.')

def db_check():
    webhook = DiscordService()
    mongo = MongoDBService()
    error_flag = False
    db1 = mongo._livePriceConn().count_documents({})
    db2 = mongo._memberInfoConn().count_documents({})
    db_lst = [db1, db2]
    for i in range(len(db_lst)):
        if db_lst[i] >= 10:
            webhook.stableCheck(f'Staging: db{i+1} is unstable, please check it.')
            error_flag = True
    if error_flag == False:
        webhook.stableCheck('Staging: All dbs are stable.')


### to-do: add ohlcv to mongodb
job1_id = 'binance_btc_websocket'
job2_id = 'binance_eth_websocket'
job3_id = 'binance_all_market_websocket'
scheduler.add_job(safe_run, 'date', id=job1_id, run_date=datetime.now(), args=[job1_id])
scheduler.add_job(safe_run, 'date', id=job2_id, run_date=datetime.now(), args=[job2_id])
# scheduler.add_job(aras_account_websocket, 'interval', minutes=30, next_run_time=datetime.now()+timedelta(seconds=3))
# scheduler.add_job(yuan_account_websocket, 'interval', minutes=30, next_run_time=datetime.now()+timedelta(seconds=3))
scheduler.add_job(safe_run, 'date', id=job3_id, run_date=datetime.now(), args=[job3_id])
scheduler.add_job(YuanIndicatorSignal, 'interval', seconds=10, next_run_time=datetime.now()+timedelta(seconds=2))
scheduler.add_job(stable_check, 'interval', hours=8, next_run_time=datetime.now()+timedelta(seconds=10))
scheduler.add_job(leverage_updater, 'interval', days=3, next_run_time=datetime.now()+timedelta(seconds=10))
scheduler.start()