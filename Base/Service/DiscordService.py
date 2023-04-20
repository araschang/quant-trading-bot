from discord import SyncWebhook
from Base.ConfigReader import Config


class Connector(object):
    def __init__(self):
        self.config = Config()


class DiscordService(Connector):
    def __init__(self):
        super().__init__()
        config = self.config['Discord']
        self.webhook = config['webhook_url']
        self.btc_hedge = config['btc_hedge']
        self.webhookForStableCheck = config['stable_check']

    def sendMessage(self, message):
        webhook = SyncWebhook.from_url(self.webhook)
        webhook.send(message)

    def stableCheck(self, message):
        webhook = SyncWebhook.from_url(self.webhookForStableCheck)
        webhook.send(message)

    def btc_hedge(self, message):
        webhook = SyncWebhook.from_url(self.btc_hedge)
        webhook.send(message)
