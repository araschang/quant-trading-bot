import pandas as pd
import ccxt
import os
from Base.ConfigReader import Config
from Application.Api.Service.DiscordService import DiscordService

class Connector(object):
    def __init__(self):
        self.config = Config()

class YuanIndicator(Connector):
    def __init__(self, symbol, exchange, api_key=None, api_secret=None, strategy=None):
        super().__init__()
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.exchange_name = exchange
        self.strategy = strategy
        self.discord = DiscordService()
        config = self.config['Binance']
        if self.api_key == config['api_key']:
            self.name = 'Aras'
        else:
            self.name = 'Yuan'
        if exchange == 'binance':
            self.exchange = ccxt.binanceusdm({
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'enableRateLimit': True,
                'option': {
                    'defaultMarket': 'future',
                },
            })
        elif exchange == 'bybit':
            self.exchange = ccxt.bybit({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'enableRateLimit': True,
        })
        self.STOPLOSS_STAGE = [0.0075, 0.025, 0.055, 0.085, 100]

    def getOHLCV(self, timeframe):
        '''
        Get OHLCV data from exchange
        Return a dataframe with OHLCV data
        '''
        ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe, limit=100)
        df = pd.DataFrame(ohlcv, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        return df
    
    def cleanData2GenerateMeanVolume(self, ohlcv_df):
        '''
        Clean data to generate mean volume
        Return a dataframe with mean volume
        '''
        volume = ohlcv_df['volume']
        Q1 = volume.quantile(0.25)
        Q3 = volume.quantile(0.75)
        IQR = Q3 - Q1
        volume_mean = volume[(volume >= Q1 - 1.5 * IQR) & (volume <= Q3 + 1.5 * IQR)].mean()
        return volume_mean
    
    def checkSignal(self, mean_volume, ohlcv_df):
        '''
        Check signal
        Return a dataframe with signal
        '''
        if self.exchange_name == 'binance':
            symbol = self.symbol.split('/')
            symbol = symbol[0] + symbol[1]
        elif self.exchange_name == 'bybit':
            symbol = self.symbol

        if ohlcv_df['volume'].iloc[-1] >= mean_volume * 8:
            slope = ohlcv_df['close'].iloc[-1] - ohlcv_df['close'].iloc[-10]
            trend = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTrend.csv'))['trend'].iloc[-1]
            try:
                close_df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanClose.csv'))
                check_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
            except Exception as e:
                ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                check_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
            
            index = list(close_df[(df['API_KEY'] == self.api_key) & (close_df['SYMBOL'] == self.symbol) & (close_df['STRATEGY'] == self.strategy)].index)[0]
            # if slope <= 0:
            if slope <= 0 and trend == 'up':
                if (str(check_df['time'].iloc[-1]) != str(ohlcv_df['time'].iloc[-1])) and (str(close_df['TIME'].iloc[index]) != str(ohlcv_df['time'].iloc[-1])):
                    ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                    # self.discord.sendMessage(f'**{symbol}** BUY!')
                    return 'buy'
                else:
                    return ''
            elif slope > 0 and trend == 'down':
            # elif slope > 0:
                if (str(check_df['time'].iloc[-1]) != str(ohlcv_df['time'].iloc[-1])) and (str(close_df['TIME'].iloc[index]) != str(ohlcv_df['time'].iloc[-1])):
                    ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                    # self.discord.sendMessage(f'**{symbol}** SELL!')
                    return 'sell'
                else:
                    return ''
    
    def openPosition(self, ohlcv, side, assetPercent, leverage: int, now_price: float, stoplossPercent) -> None:
        '''
        Open position
        Return a dataframe with open position
        '''
        if side != 'buy' and side != 'sell':
            return
        
        transaction = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'))
        if len(transaction[(transaction['API_KEY'] == self.api_key) & (transaction['SYMBOL'] == self.symbol) & (transaction['STRATEGY'] == self.strategy)]) > 0:
            return

        if self.exchange_name == 'binance':
            if self.symbol[:3] == 'BTC':
                round_digit = 1
            elif self.symbol[:3] == 'ETH':
                round_digit = 2
        elif self.exchange_name == 'bybit':
            if self.symbol[:3] == 'BTC':
                round_digit = 1
            elif self.symbol[:3] == 'ETH':
                round_digit = 2
        ohlcv['ATR'] = self.ATR(ohlcv, 14)
        wallet_balance = float(self.exchange.fetch_balance()['info']['totalWalletBalance'])
        amount = round(wallet_balance * assetPercent / now_price, 3)
        self.exchange.create_market_order(self.symbol, side, amount)
        self.discord.sendMessage(f'**{self.symbol}** {self.name} {side.upper()} {amount} at {now_price}')
        if self.exchange_name == 'binance':
            now_price = float(self.exchange.fetch_positions([self.symbol])[0]['info']['entryPrice'])
        elif self.exchange_name == 'bybit':
            now_price = float(self.exchange.fetch_positions(self.symbol)[0]['info']['entry_price'])
        atr = float(ohlcv['ATR'].iloc[-1])
        self.insertTransationData(side, amount, now_price, atr, 0)

        if side == 'buy':
            stop_loss_side = 'sell'
            stop_loss_price = round(now_price - (2.5 * atr), round_digit)
            take_profit_price = round(now_price + (5 * atr), round_digit)
        else:
            stop_loss_side = 'buy'
            stop_loss_price = round(now_price + (2.5 * atr), round_digit)
            take_profit_price = round(now_price - (5 * atr), round_digit)

        try:
            if self.exchange_name == 'binance':
                self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price, 'closePosition': True})
                if self.strategy == 'YuanCopyTrade':
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'takeProfitPrice': take_profit_price, 'closePosition': True})
            elif self.exchange_name == 'bybit':
                if stop_loss_side == 'buy':
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'takeProfitPrice': stop_loss_price})
                else:
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price})
            return 'Open position success'
        except Exception:
            now_price = float(self.getOHLCV('3m')['close'].iloc[-1])
            if side == 'buy':
                stop_loss_price = round(now_price - (2.5 * atr), round_digit)
                take_profit_price = round(now_price + (5 * atr), round_digit)
            else:
                stop_loss_price = round(now_price + (2.5 * atr), round_digit)
                take_profit_price = round(now_price - (5 * atr), round_digit)
            
            if self.exchange_name == 'binance':
                self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price, 'closePosition': True})
                if self.strategy == 'YuanCopyTrade':
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'takeProfitPrice': take_profit_price, 'closePosition': True})
            elif self.exchange_name == 'bybit':
                if stop_loss_side == 'buy':
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'takeProfitPrice': stop_loss_price})
                else:
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price})
            return 'Open position success'
    
    def changeStopLoss(self, price):
        '''
        Change stop loss
        Return a dataframe with change stop loss
        '''
        df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'))
        side = df[(df['API_KEY'] == self.api_key) & (df['SYMBOL'] == self.symbol) & (df['STRATEGY'] == self.strategy)]['SIDE'].iloc[0]
        order_info = self.exchange.fetch_open_orders(self.symbol)
        if side == 'buy':
            stop_loss_side = 'sell'
        else:
            stop_loss_side = 'buy'
        
        for i in range(len(order_info)):
            if order_info[i]['info']['type'] == 'STOP_MARKET':
                orderId = order_info[i]['info']['orderId']
                break

        self.exchange.cancel_order(orderId, self.symbol)
        self.exchange.create_market_order(self.symbol, stop_loss_side, 1, params={'stopLossPrice': price, 'closePosition': True})
        
    def checkIfThereIsStopLoss(self, now_price, ohlcv):
        if self.exchange_name == 'binance':
            position = self.exchange.fetch_positions([str(self.symbol)])
            has_position = float(position[0]['info']['positionAmt'])
            df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'))
            if self.strategy == 'Yuan':
                if has_position > 0: # if long
                    position_index = list(df[(df['API_KEY'] == self.api_key) & (df['SYMBOL'] == self.symbol) & (df['STRATEGY'] == self.strategy)].index)[0]
                    price = float(df['PRICE'].iloc[position_index])
                    change = round((now_price - price) / price, 4)
                    stoploss_stage = int(df['STOPLOSS_STAGE'].iloc[position_index])
                    if change >= self.STOPLOSS_STAGE[stoploss_stage]:
                        if stoploss_stage == 0:
                            stoploss_price = price + 0.0008 * price
                            self.changeStopLoss(stoploss_price)
                            df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                            df['STOPLOSS_STAGE'].iloc[df_index] = 1
                        elif stoploss_stage == 1:
                            stoploss_price = price + 0.01 * price
                            self.changeStopLoss(stoploss_price)
                            df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                            df['STOPLOSS_STAGE'].iloc[df_index] = 2
                        elif stoploss_stage == 2:
                            stoploss_price = price + 0.02 * price
                            self.changeStopLoss(stoploss_price)
                            df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                            df['STOPLOSS_STAGE'].iloc[df_index] = 3
                        elif stoploss_stage == 3:
                            stoploss_price = price + 0.04 * price
                            self.changeStopLoss(stoploss_price)
                            df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                            df['STOPLOSS_STAGE'].iloc[df_index] = 4
                        df.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'), index=False)

                elif has_position < 0: # if short
                    position_index = list(df[(df['API_KEY'] == self.api_key) & (df['SYMBOL'] == self.symbol) & (df['STRATEGY'] == self.strategy)].index)[0]
                    price = float(df['PRICE'].iloc[position_index])
                    change = round((price - now_price) / price, 4)
                    stoploss_stage = int(df['STOPLOSS_STAGE'].iloc[position_index])
                    if change >= self.STOPLOSS_STAGE[stoploss_stage]:
                        if stoploss_stage == 0:
                            stoploss_price = price - 0.0008 * price
                            self.changeStopLoss(stoploss_price)
                            df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                            df['STOPLOSS_STAGE'].iloc[df_index] = 1
                        elif stoploss_stage == 1:
                            stoploss_price = price - 0.01 * price
                            self.changeStopLoss(stoploss_price)
                            df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                            df['STOPLOSS_STAGE'].iloc[df_index] = 2
                        elif stoploss_stage == 2:
                            stoploss_price = price - 0.02 * price
                            self.changeStopLoss(stoploss_price)
                            df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                            df['STOPLOSS_STAGE'].iloc[df_index] = 3
                        elif stoploss_stage == 3:
                            stoploss_price = price - 0.04 * price
                            self.changeStopLoss(stoploss_price)
                            df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                            df['STOPLOSS_STAGE'].iloc[df_index] = 4
                        df.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'), index=False)
                else:
                    self.deleteTransationData()

            elif self.strategy == 'YuanCopyTrade':
                if has_position > 0:
                    position_index = list(df[(df['API_KEY'] == self.api_key) & (df['SYMBOL'] == self.symbol) & (df['STRATEGY'] == self.strategy)].index)[0]
                    price = float(df['PRICE'].iloc[position_index])
                    atr = float(df['ATR'].iloc[position_index])
                    stoploss_stage = int(df['STOPLOSS_STAGE'].iloc[position_index])
                    amount = float(df['AMOUNT'].iloc[position_index])
                    if now_price >= round(price + 1 * atr, 4) and stoploss_stage == 0:
                        stoploss_price = round(price + 0.0009 * price, 2)
                        self.changeStopLoss(stoploss_price)
                        self.discord.sendMessage(f'**{self.symbol}** {self.name} Stoploss Stage 1, Protect Original Price')
                        df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                        df['STOPLOSS_STAGE'].iloc[df_index] = 1
                        df.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'), index=False)
                    elif now_price >= round(price + 2.5 * atr, 4) and stoploss_stage == 1:
                        self.exchange.create_market_order(self.symbol, 'sell', round(amount / 2, 3))
                        self.discord.sendMessage(f'**{self.symbol}** {self.name} Stoploss Stage 2, Sell Half')
                        stoploss_price = round(price + 2 * atr, 2)
                        try:
                            self.changeStopLoss(stoploss_price)
                        except:
                            self.exchange.create_market_order(self.symbol, 'sell', round(amount / 2, 3))
                        df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                        df['STOPLOSS_STAGE'].iloc[df_index] = 2
                        df.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'), index=False)
                    
                elif has_position < 0:
                    position_index = list(df[(df['API_KEY'] == self.api_key) & (df['SYMBOL'] == self.symbol) & (df['STRATEGY'] == self.strategy)].index)[0]
                    price = float(df['PRICE'].iloc[position_index])
                    atr = float(df['ATR'].iloc[position_index])
                    stoploss_stage = int(df['STOPLOSS_STAGE'].iloc[position_index])
                    amount = float(df['AMOUNT'].iloc[position_index])
                    if now_price <= round(price - 1 * atr, 4) and stoploss_stage == 0:
                        stoploss_price = round(price - 0.0009 * price, 2)
                        self.changeStopLoss(stoploss_price)
                        self.discord.sendMessage(f'**{self.symbol}** {self.name} Stoploss Stage 1, Protect Original Price')
                        df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                        df['STOPLOSS_STAGE'].iloc[df_index] = 1
                        df.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'), index=False)
                    elif now_price <= round(price - 2.5 * atr, 4) and stoploss_stage == 1:
                        self.exchange.create_market_order(self.symbol, 'buy', round(amount / 2, 3))
                        self.discord.sendMessage(f'**{self.symbol}** {self.name} Stoploss Stage 2, Buy Half')
                        stoploss_price = round(price - 2 * atr, 2)
                        try:
                            self.changeStopLoss(stoploss_price)
                        except:
                            self.exchange.create_market_order(self.symbol, 'buy', round(amount / 2, 3))
                        df_index = list(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)].index)[0]
                        df['STOPLOSS_STAGE'].iloc[df_index] = 2
                        df.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'), index=False)
                else:
                    count = len(df.loc[(df['SYMBOL'] == self.symbol) & (df['API_KEY'] == self.api_key) & (df['STRATEGY'] == self.strategy)])
                    if count > 0:
                        df_close = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanClose.csv'))
                        index = list(df_close.loc[(df_close['SYMBOL'] == self.symbol) & (df_close['API_KEY'] == self.api_key) & (df_close['STRATEGY'] == self.strategy)].index)[0]
                        df_close['TIME'].iloc[index] = ohlcv['time'].iloc[-1]
                        df_close.to_csv(os.path.join(os.path.dirname(__file__), 'YuanClose.csv'), index=False)
                    self.deleteTransationData()
                    self.cancelOrder()

    def closePosition(self):
        '''
        Close position
        Return a dataframe with close position
        '''
        if self.exchange_name == 'binance':
            amount = float(self.exchange.fetch_positions([str(self.symbol)])[0]['info']['positionAmt'])
            if amount > 0:
                side = 'sell'
            else:
                side = 'buy'
        elif self.exchange_name == 'bybit':
            amount = float(self.exchange.fetch_positions(self.symbol)[0]['info']['size'])
            side = self.exchange.fetch_positions(self.symbol)[0]['info']['side']
            if side == 'Buy':
                side = 'sell'
            else:
                side = 'buy'
        self.exchange.create_market_order(self.symbol, side, amount)
    
    def checkIfNoPositionCancelOpenOrder(self):
        '''
        Check if no position cancel open order
        Return a dataframe with check if no position cancel open order
        '''
        if self.exchange_name == 'binance':
            position = self.exchange.fetch_positions([str(self.symbol)])
            has_position = float(position[0]['info']['positionAmt'])
            if has_position == 0:
                if len(self.exchange.fetch_open_orders(self.symbol)) != 0:
                    self.cancelOrder()
            else:
                if len(self.exchange.fetch_open_orders(self.symbol)) < 2:
                    self.cancelOrder()
                    df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'))
                    position_index = list(df[(df['API_KEY'] == self.api_key) & (df['SYMBOL'] == self.symbol) & (df['STRATEGY'] == self.strategy)].index)[0]
                    price = float(df['PRICE'].iloc[position_index])
                    atr = float(df['ATR'].iloc[position_index])
                    side = df['SIDE'].iloc[position_index]
                    amount = float(df['AMOUNT'].iloc[position_index])
                    if side == 'buy':
                        stop_side = 'sell'
                        stop_loss_price = round(price - 2.5 * atr, 2)
                        take_profit_price = round(price + 4 * atr, 2)
                    else:
                        stop_side = 'buy'
                        stop_loss_price = round(price + 2.5 * atr, 2)
                        take_profit_price = round(price - 4 * atr, 2)
                    self.exchange.create_market_order(self.symbol, stop_side, amount, params={'stopLossPrice': stop_loss_price, 'closePosition': True})
                    self.exchange.create_market_order(self.symbol, stop_side, amount, params={'takeProfitPrice': take_profit_price, 'closePosition': True})

                    
        elif self.exchange_name == 'bybit':
            if len(self.exchange.fetch_positions(self.symbol)) == 0:
                if len(self.exchange.fetch_open_orders(self.symbol)) != 0:
                    self.cancelOrder()

    def cancelOrder(self):
        '''
        Cancel order
        Return a dataframe with cancel order
        '''
        try:
            orderId = self.exchange.fetch_open_orders(self.symbol)[0]['info']['orderId']
            self.exchange.cancel_order(orderId, self.symbol)
        except:
            pass

    def checkTrend(self):
        '''
        Check trend
        Return a dataframe with trend
        '''
        ohlcv_df = self.getOHLCV('4h')
        ohlcv_df['short_ma'] = ohlcv_df['close'].ewm(com=20, min_periods=20).mean()
        ohlcv_df['long_ma'] = ohlcv_df['close'].ewm(com=40, min_periods=40).mean()
        if ohlcv_df['short_ma'].iloc[-1] < ohlcv_df['long_ma'].iloc[-1]:
            trend = pd.DataFrame({'trend': ['down']})
            trend.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTrend.csv'))
            return 'down'
        else:
            trend = pd.DataFrame({'trend': ['up']})
            trend.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTrend.csv'))
            return 'up'

    def insertTransationData(self, side, amount, price, ATR, stoploss_stage):
        df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'))
        df.loc[len(df)] = [self.api_key, self.symbol, side, amount, price, ATR, self.strategy, stoploss_stage]
        df.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'), index=False)
    
    def deleteTransationData(self):
        df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'))
        user_transaction_list = list(df[df['API_KEY'] == self.api_key].index)
        if len(user_transaction_list) == 0:
            return
        for i in user_transaction_list:
            if (df['SYMBOL'].iloc[i] == self.symbol) and (df['STRATEGY'].iloc[i] == self.strategy):
                df.drop(i, inplace=True)
                self.discord.sendMessage(f'**{self.symbol}** {self.name} position closed.')
                break
        df.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'), index=False)

    def ATR(self, DF, n=14):
        df = DF.copy()
        df['H-L'] = df['high'] - df['low']
        df['H-PC'] = abs(df['high'] - df['close'].shift(1))
        df['L-PC'] = abs(df['low'] - df['close'].shift(1))
        df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1, skipna=False)
        df['ATR'] = df['TR'].ewm(span=n, min_periods=n).mean()
        return df['ATR']

if __name__ == '__main__':
    symbol = 'ETH/USDT'
    strategy = 'YuanCopyTrade'
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTransaction.csv'))