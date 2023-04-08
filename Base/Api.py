from flask_restful import Api
from flask import Flask
from Model import Scheduler
from Indicators.Scheduler import YuanScheduler

app = Flask(__name__)
api = Api(app)
