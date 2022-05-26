#!/usr/bin/env python
# coding: utf-8

import backtrader as bt
from datetime import datetime,timedelta
import pandas as pd

import numpy as np

       
class PandasDataOptix(bt.feeds.PandasData):
    lines = ('code', )
    params = (('code', 0),)

class StrategyOptix(bt.Strategy):
    def __init__(self):
        self.index = {} # Contains the key value pair for all the loaded data
        self.portfolio_value ={} # Contains daily portfolio value at the close
        self.portfolio_composition ={} # Contains the top ranked stocks selected from the entire universe
        self.inds = {} 
        self.start_date=datetime(2019, 6, 1).date() # Start date
        self.pos_data=[] 
        self.period = 100 # The lookback period
        self.max_size = 20 # Max number of equiweighted stock in the portfolio
        self.print_log = False # Prints the log of the data
        
        for stock in range(0,len(self.datas)):
            code = self.datas[stock].code[0]
            self.index[int(code)]=stock
            self.inds[code] = {}
            self.inds[code]["momentum"] = Returns(self.datas[stock].close,period=self.period)
        self.add_timer(
            bt.timer.SESSION_START,  # when it will be called
            monthdays=[1],  # called on the 1st day of the month
            monthcarry=True,  # called on the 2nd day if the 1st is holiday
            tzdata=self.datas[0],
        )
    def log(self, txt, dt=None):
        if self.print_log:
            dt = dt or self.data.datetime[0]
            dt = bt.num2date(dt)
            print('%s, %s' % (dt.isoformat(), txt))

    
    def notify_timer(self, timer, when):
        self.log('strategy notify_timer with tid {}, when {} cheat {}'.
              format(timer.p.tid, when, timer.p.cheat))
        if when.date()>=self.start_date  :
            self.rebalance_portfolio()
            self.portfolio_composition.update({'{}'.format(when.date()):self.max_size})
#         remove those no longer top ranked
#         do this first to issue sell orders and free cash
            for d in (d for d in self.pos_data if d not in self.top):
                self.log('Leave {} '.format(d))
                index=(self.index.get(int(d)))
                self.order_target_percent(self.datas[index], target=0.0)
            for d in self.top:
                self.log('Enter {} '.format(d))
                index=(self.index.get(int(d)))
                self.order_target_percent(self.datas[index], target=0.05)

            self.pos_data=self.top
    
    
    def next(self):
        self.portfolio_value[self.data.datetime.date(0)]=self.broker.getvalue()
    
    def notify_order(self, order):
        if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
            return  

        if order.status == order.Completed:
            if order.isbuy():
                buytxt = 'BUY COMPLETE, %.2f' % order.executed.price
                self.log(buytxt, order.executed.dt)
            else:
                selltxt = 'SELL COMPLETE, %.2f' % order.executed.price
                self.log(selltxt, order.executed.dt)

        elif order.status in [order.Expired, order.Canceled, order.Margin]:
            self.log('%s ,' % order.Status[order.status])
            pass  

        # Allow new orders
        self.orderid = None
        
    def rebalance_portfolio(self):
        self.returns_df = pd.DataFrame(columns=['codes','returns'])
        for stock in range(len(self.datas)):
            code = (self.datas[stock].code[0])
            to_append = list([(code),(self.inds[code]["momentum"].returns[0])])
            x=pd.Series(to_append,index=self.returns_df.columns)
            self.returns_df = self.returns_df.append(x, ignore_index=True)
        self.returns_df=(self.returns_df.sort_values(by='returns',ascending=False))

        self.top = list(self.returns_df['codes'][:(self.max_size)])
        
    def stop(self):
        df = pd.DataFrame.from_dict(self.portfolio_composition,orient = 'index')
        df1 = pd.DataFrame.from_dict(self.portfolio_value, orient = 'index')
        df1=(df1[self.start_date:])
        df.to_excel("portfolio_composition.xlsx")
        df1.to_excel("portfolio_value.xlsx")
        
class Returns(bt.Indicator):
    params = (
        ("period", 1),
    )
    lines = ('returns','diff',)

    def next(self):
        # This makes sure enough bars have passed before trying to calcualte the log return.
        if len(self) < self.p.period:
            return
        self.lines.returns[0]= ((self.data - self.data[-(self.p.period-1)])/
                                self.data[-(self.p.period-1)])*100
        self.lines.diff[0]= ((self.data - self.data[-(self.p.period-1)]))
    

cerebro = bt.Cerebro(stdstats=True)
cerebro.broker.set_coc(True)
cerebro.broker.setcash(1000000.0)

df=pd.read_excel('Nifty_100.xlsx',skiprows=[0],header=1,skipfooter=7,engine='openpyxl')
df.set_index('NDP_Date', inplace=True)
df.drop(df.columns[[0, 2, 3,4,5,7,8,9]], axis = 1, inplace = True)
codes=df['Accord Code'].unique()

ranked_df = pd.DataFrame()
run=1
for code in codes[:]:
    bj=df.loc[df['Accord Code'] == code]
    bj = bj.sort_index()
    bj=bj.rename({ 'NDP_Close': 'close'}, axis=1)  # new method
    data = PandasDataOptix(dataname=bj)
    cerebro.adddata(data)
cerebro.addobserver(bt.observers.Value)
cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, )
cerebro.addanalyzer(bt.analyzers.TimeReturn, timeframe=bt.TimeFrame.Years)
cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.05, _name='sharpe')
cerebro.addanalyzer(bt.analyzers.Returns)
cerebro.addanalyzer(bt.analyzers.DrawDown)
cerebro.addstrategy(StrategyOptix)

cerebro.addwriter(bt.WriterFile, rounding=4,csv=True, out = "BT_Results.csv")
results = cerebro.run()
result = results[0]

# print('Sharpe Ratio:', result.analyzers.sharpe.get_analysis())
portvalue = cerebro.broker.getvalue()
print('Final portfolio value :', portvalue)





