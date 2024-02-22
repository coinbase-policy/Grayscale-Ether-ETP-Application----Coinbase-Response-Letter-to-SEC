##############################################################################################
# Trading Volume and BA spread BTC/ETH vs S&P500 Constituents.  
# Feb 2024
# Coinbase Policy
# Coinbase Response Letter to the Grayscale ETH ETF Application
##############################################################################################

##############################################################################################
##############################################################################################
# DOWNLOAD ORDER BOOK BID ASK  FOR ETH AND BTC

sql = """ select * from (
select  max(price) as bid , book as buy_book, timestamp as timestamp_buy, side as buyside
from ANALYTICS.moonbase_engine.ORDER_BOOK_SNAPSHOTS
where (book = 'BTC-USD' OR book = 'ETH-USD' OR book = 'SOL-USD'  OR book = 'XRP-USD' OR book = 'ADA-USD' OR book = 'AVAX-USD' OR book = 'DOGE-USD' OR book = 'LINK-USD' OR book = 'DOT-USD' OR book = 'MATIC-USD')    and timestamp> '2024-01-01 00:00:00' and side = 'buy'
group by book, timestamp, buyside ) as buyside
join (
select  min(price) as ask, book as sell_book, timestamp as timestamp_sell, side as sellside
from ANALYTICS.moonbase_engine.ORDER_BOOK_SNAPSHOTS
where (book = 'BTC-USD' OR book = 'ETH-USD' OR book = 'SOL-USD'  OR book = 'XRP-USD' OR book = 'ADA-USD' OR book = 'AVAX-USD' OR book = 'DOGE-USD' OR book = 'LINK-USD' OR book = 'DOT-USD' OR book = 'MATIC-USD')     and timestamp> '2024-01-01 00:00:00' and side = 'sell'
group by book, timestamp, sellside) as sellside on buyside.buy_book=sellside.sell_book and buyside.timestamp_buy = sellside.timestamp_sell
 """

df = cb_databricks.snowflake.load_as_spark_dataframe(sql, use_lowercase_column_names=True)

##############################################################################################
# Compute BID ASK SPREAD

df = df.withColumn('spread', df.ask - df.bid)
df = df.withColumn('midprice', (df.ask + df.bid) / 2)
df = df.withColumn('perc_spread',  df.spread / df.midprice)
df = df.select('spread', 'midprice', 'sell_book', 'timestamp_sell', 'perc_spread')
df = df.select('spread','perc_spread', 'sell_book',F.date_format('timestamp_sell','yyyy-MM-dd').alias('day'))
df = df.groupBy('day','sell_book').avg()

##############################################################################################
# Compute BID ASK PERCENTAGE SPREAD

first_eth = df.filter(col('sell_book') == 'ETH-USD').orderBy('day').first()['avg(perc_spread)']
display(first_eth)
first_btc = df.filter(col('sell_book') == 'BTC-USD').orderBy('day').first()['avg(perc_spread)']
display(first_btc)
first_xrp = df.filter(col('sell_book') == 'XRP-USD').orderBy('day').first()['avg(perc_spread)']
display(first_xrp)
first_ada = df.filter(col('sell_book') == 'ADA-USD').orderBy('day').first()['avg(perc_spread)']
display(first_ada)
first_avax = df.filter(col('sell_book') == 'AVAX-USD').orderBy('day').first()['avg(perc_spread)']
display(first_avax)
first_sol = df.filter(col('sell_book') == 'SOL-USD').orderBy('day').first()['avg(perc_spread)']
display(first_sol)
first_doge = df.filter(col('sell_book') == 'DOGE-USD').orderBy('day').first()['avg(perc_spread)']
display(first_doge)
first_link = df.filter(col('sell_book') == 'LINK-USD').orderBy('day').first()['avg(perc_spread)']
display(first_link)
first_dot = df.filter(col('sell_book') == 'DOT-USD').orderBy('day').first()['avg(perc_spread)']
display(first_dot)
first_matic = df.filter(col('sell_book') == 'MATIC-USD').orderBy('day').first()['avg(perc_spread)']
display(first_matic)


##############################################################################################
# Average Percentage Spread over time.

eth = df.filter(col('sell_book') == 'ETH-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_eth))
btc = df.filter(col('sell_book') == 'BTC-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_btc))
xrp = df.filter(col('sell_book') == 'XRP-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_xrp))
sol = df.filter(col('sell_book') == 'SOL-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_sol))
ada = df.filter(col('sell_book') == 'ADA-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_ada))
avax = df.filter(col('sell_book') == 'AVAX-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_avax))
doge = df.filter(col('sell_book') == 'DOGE-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_doge))
link = df.filter(col('sell_book') == 'LINK-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_link))
dot = df.filter(col('sell_book') == 'DOT-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_dot))
matic = df.filter(col('sell_book') == 'MATIC-USD').orderBy('day').withColumn('spread_st', col('avg(perc_spread)') *100 / lit(first_matic))
df2 = eth.union(btc)
df2 = df2.union(xrp)
df2 = df2.union(sol)
df2 = df2.union(ada)
df2 = df2.union(avax)
df2 = df2.union(doge)
df2 = df2.union(link)
df2 = df2.union(dot)
df2 = df2.union(matic)

display(df2.groupBy('sell_book').avg())






##############################################################################################
##############################################################################################
# PLOT BID ASK SPREAD PERCENTAGE FOR SP500 and ETH/BTC

##############################################################################################
#Install Packages

!pip install yfinance
!pip install pandas_datareader
import yfinance as yf
from pandas_datareader import data
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time

##############################################################################################
#Find list of S&P500 Constituents and add ETH and BTC

tickers = pd.read_html(
    'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
print(tickers.head())

tickers_list = tickers['Symbol'].to_list()
tickers_list.append('BTC-USD')
tickers_list.append('ETH-USD')

##############################################################################################
#Download data about market cap and volume from Yahoo Finance

df = pd.DataFrame(columns = ['ticker', 'mcap', 'volume10', 'perc_spread', 'bid', 'ask', 'flag_spread'])
tickers_list_final = []
for ticker in tickers_list:
  info = yf.Ticker(ticker).info
  try:
    mcap = info['marketCap']
    if (ticker == 'ETH-USD') | (ticker == 'BTC-USD'):
      vol10 = info["averageVolume10days"]

      df.loc[len(df)] = [ticker, mcap, vol10, np.nan , np.nan, np.nan, np.nan ]

    else:
      vol10 = info["averageVolume10days"] * info.get('regularMarketPrice', info.get('currentPrice'))
      df.loc[len(df)] = [ticker, mcap, vol10, np.nan , np.nan, np.nan, np.nan ]
    tickers_list_final.append(ticker)

  except:
    pass

##############################################################################################
#Plot Trading Volume Scatter Plot


fig, ax = plt.subplots()

colors = ['k'] * len(tickers_list_final)
colors[-1] = 'r'
colors[-2] = 'r'
df.plot.scatter(x='mcap', y='volume10', c=colors, ax = ax)
plt.yscale('log')
plt.xscale('log')
plt.title('Trading Volume: S&P500 Constit. Vs BTC/ETH')
plt.ylabel('Daily Trading Volume ($)')
plt.xlabel('Market Cap ($)')


for i, txt in enumerate(tickers_list_final):
    if i > len(tickers_list_final) -3:
      ax.annotate(txt[:3], (df['mcap'][i], df['volume10'][i]))
    else:
      if df['volume10'][i]>9000000000:
        ax.annotate(txt, (df['mcap'][i], df['volume10'][i]))



df.to_csv('trading_volume_SP')



##############################################################################################
#Download data about market cap and bid ask spread from Yahoo Finance


for i in range(60):
  time.sleep(60)
  df = pd.DataFrame(columns = ['ticker', 'mcap', 'perc_spread', 'bid', 'ask', 'flag_spread'])
  tickers_list_final = []
  for ticker in tickers_list:
    print(ticker)
    try:
      info = yf.Ticker(ticker).info
      mcap = info['marketCap']
      if (ticker == 'ETH-USD') | (ticker == 'BTC-USD'):
        vol10 = info["averageVolume10days"]

        df.loc[len(df)] = [ticker, mcap,  np.nan , np.nan, np.nan, np.nan ]

      else:
        vol10 = info["averageVolume10days"] * info.get('regularMarketPrice', info.get('currentPrice'))
        bid = info['bid']
        ask = info['ask']
        spread = ask - bid
        perc_spread = 2 * spread / (ask + bid)
        flag_spread = 0
        if spread <=0.01 :
          flag_spread = 1
        df.loc[len(df)] = [ticker, mcap, perc_spread, bid, ask, flag_spread]

      tickers_list_final.append(ticker)

    except:
      pass

  df_tot['perc_spread'] = (df_tot['perc_spread'] * (i+1) + df['perc_spread']) / (i+2)
  df_tot['flag_spread'] = (df_tot['flag_spread'] * (i+1) + df['flag_spread']) / (i+2)


##############################################################################################
#Data about ETH AND BTC (See above)

df_tot.loc[df.ticker == 'ETH-USD', 'perc_spread'] = 0.00005
df_tot.loc[df.ticker == 'BTC-USD', 'perc_spread'] = 0.000047
df = df_tot

##############################################################################################
#PLOT SCATTERPLOT of BA SPREAD PERC vs Marketcap. 

fig, ax = plt.subplots()
df['col'] = np.where(df['flag_spread'] >0.5 , 'b', 'k')
colors = df['col'].to_list()
colors[-1] = 'r'
colors[-2] = 'r'
df.plot.scatter(x='mcap', y='perc_spread', c=colors, ax = ax)
plt.yscale('log')
plt.xscale('log')
plt.title('Bid-Ask Spread Perc: S&P500 Constit. Vs BTC/ETH')
plt.ylabel('Bid-Ask Spread (%)')
plt.xlabel('Market Cap ($)')


for i, txt in enumerate(tickers_list_final):
    if i > len(tickers_list_final) -3:
      ax.annotate(txt[:3], (df['mcap'][i], df['perc_spread'][i]))
    else:
      if df['perc_spread'][i]<0.0001:
        ax.annotate(txt, (df['mcap'][i], df['perc_spread'][i]))

plt.text(0.13, -0.17, 'Blue and black dots represents stocks in the S&P500 index. \nRed dots are ether and bitcoin. \nStocks with the blue dot have a bid-ask spread of $1 cent. \nB\\A spreads are measured on Feb 21, 2024.', ha='left', wrap=True, transform=plt.gcf().transFigure)

df.to_csv('badata.csv')
