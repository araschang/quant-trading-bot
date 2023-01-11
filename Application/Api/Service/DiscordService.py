from discord import SyncWebhook
# import sys
# sys.path.append('./')
from Base.ConfigReader import Config


class Connector(object):
    def __init__(self):
        self.config = Config()


class DiscordService(Connector):
    def __init__(self):
        super().__init__()
        config = self.config['Discord']
        self.webhook = config['webhook_url']

    def sendMessage(self, message):
        webhook = SyncWebhook.from_url(self.webhook)
        webhook.send(message)

if __name__ == '__main__':
    discordService = DiscordService()
    discordService.sendMessage("Hello World!")
