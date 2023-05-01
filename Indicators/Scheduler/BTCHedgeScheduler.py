from apscheduler.schedulers.background import BackgroundScheduler
from Base.ConfigReader import Config
from Indicators.BTCHedge import BTCHedge
from Model.Service.MongoDBService import MongoDBService
from datetime import datetime, timedelta
import logging

logging.basicConfig(filename='quantlog.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
scheduler = BackgroundScheduler(job_defaults={'max_instances': 1})
config = Config()
mongo = MongoDBService()
query = {'STRATEGY': 'BTCHedge'}
member = list(mongo._memberInfoConn().find(query))

def detect_signal(member):
    try:
        for i in range(len(member)):
            api_key = member[i]['API_KEY']
            api_secret = member[i]['API_SECRET']
            order_qty = member[i]['ORDER_QTY']
            indicator = BTCHedge('binance', api_key, api_secret, order_qty)
            indicator.signalGenerator()
    except Exception as e:
        logging.error('An error occurred in BTCHedge signalGenerator: %s', e, exc_info=True)
        print(e)
    print('DETECT BTC HEDGE SIGNAL DONE')

scheduler.add_job(detect_signal, 'interval', seconds=0.5, args=[member], next_run_time=datetime.now() + timedelta(seconds=3))
scheduler.start()
