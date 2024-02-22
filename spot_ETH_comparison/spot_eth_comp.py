##############################################################################################
# Spot ETH prices across exchanges 
# Feb 2024
# Coinbase Policy
# Coinbase Response Letter to the Grayscale ETH ETF Application
##############################################################################################

##############################################################################################
# Download Data from Coinbase.

sql = """ select timestamp, market, close_price as close, volume, quote_volume as volume_dollar
from ANALYTICS.MARKET_DATA.CRYPTOCOMPARE_SPOT_OHLC_HOUR
where base = 'ETH' and quote = 'USD' and (market = 'kraken' or market = 'gemini' or market = 'bitstamp') and DS > '2022-12-31' 
order by timestamp """

df1 = cb_databricks.snowflake.load_as_spark_dataframe(sql, use_lowercase_column_names=True)

sql = """ select *, volume*close as volume_dollar
from ANALYTICS.EXCHANGE.HISTORIC_MARKET_DATA
where quote_currency = 'USD' and BASE_CURRENCY = 'ETH' and GRANULARITY = 'ONE_HOUR' and start_date > '2022-12-31' and source = 'EXCHANGE'
order by start_date """

df2 = cb_databricks.snowflake.load_as_spark_dataframe(sql, use_lowercase_column_names=True)
display(df2)
df1 = df1.withColumnRenamed("timestamp", "start")
df2 = df2.select('start', 'close')
df2 = df2.withColumnRenamed("close", "close_coinbase")
pdf1 = df1.pandas_api()
pdf2 = df2.pandas_api()
df = pdf1.merge(pdf2, on = ['start'], how = 'left')
display(df)
display(pdf1)
display(pdf2)


##############################################################################################
# Define correlations and absolute divergence among exchanges

df['close100']= df['close']*100 / df['close_coinbase']

df['close100_diff_abs']= abs(df['close100'] - 100)
display(df)
df['close100_diff_abs'].describe()
corr_krak = df.loc[df['market'] == 'kraken', ['close', 'close_coinbase']].corr()
corr_gem = df.loc[df['market'] == 'gemini', ['close', 'close_coinbase']].corr()
corr_bit = df.loc[df['market'] == 'bitstamp', ['close', 'close_coinbase']].corr()
corret_krak = df.loc[df['market'] == 'kraken', ['close', 'close_coinbase']].pct_change().dropna().corr()
corret_gem = df.loc[df['market'] == 'gemini', ['close', 'close_coinbase']].pct_change().dropna().corr()
corret_bit = df.loc[df['market'] == 'bitstamp', ['close', 'close_coinbase']].pct_change().dropna().corr()
absdiv_krak = df.loc[df['market'] == 'kraken', ['close100_diff_abs']].mean()
absdiv_gem = df.loc[df['market'] == 'gemini',  ['close100_diff_abs']].mean()
absdiv_bit = df.loc[df['market'] == 'bitstamp',  ['close100_diff_abs']].mean()

##############################################################################################
# how many of the dislocation in price continued in the following hours? 

df['flag97']= 0
df.loc[df['close100_diff_abs']> df['close100_diff_abs'].quantile(q=0.97),'flag97'] = 1
df = df.sort_values(['market', 'start']).reset_index(drop = True)
df['flag97lead'] = df['flag97'].shift(+1)
df['flag97'].describe()  
df_extra = df.loc[df['flag97lead'] ==1]
print(df_extra.count())
print(df_extra.loc[df['close100_diff_abs']>0.1].count())
df = df.sort_values(['market', 'start']).reset_index(drop = True)
df['flag97lead2'] = df['flag97'].shift(+2)
display(df)
df['flag97'].describe()  
df_extra = df.loc[df['flag97lead2'] ==1]
print(df_extra.count())
print(df_extra.loc[df['close100_diff_abs']>0.1].count())
df['flag97lead3'] = df['flag97'].shift(+3)
df_extra = df.loc[df['flag97lead3'] ==1]
print(df_extra.count())
print(df_extra.loc[df['close100_diff_abs']>0.1].count())
df['flag97lead4'] = df['flag97'].shift(+4)
df_extra = df.loc[df['flag97lead4'] ==1]
print(df_extra.count())
print(df_extra.loc[df['close100_diff_abs']>0.1].count())

##############################################################################################
# Plot charts

plt.rcParams.update({'font.size': 20})


fig, (ax1, ax2, ax3) = plt.subplots(1,3,figsize = (30,10))
ax1.scatter(df.loc[df['market']=='gemini',  'close_coinbase'].to_numpy(), df.loc[df['market']=='gemini',  'close'].to_numpy(), c='DarkBlue' )
ax2.scatter(df.loc[df['market']=='kraken',  'close_coinbase'].to_numpy(), df.loc[df['market']=='kraken',  'close'].to_numpy(), c='DarkBlue' )
ax3.scatter(df.loc[df['market']=='bitstamp',  'close_coinbase'].to_numpy(), df.loc[df['market']=='bitstamp',  'close'].to_numpy(), c='DarkBlue' )
ax1.set_title('Gemini Vs Coinbase ')
ax1.set_xlabel('ETH Price on Coinbase')
ax1.set_ylabel('ETH Price on Gemini')
ax2.set_title('Kraken Vs Coinbase ')
ax2.set_xlabel('ETH Price on Coinbase')
ax2.set_ylabel('ETH Price on Kraken')
ax3.set_title('Bitstamp Vs Coinbase ')
ax3.set_xlabel('ETH Price on Coinbase')
ax3.set_ylabel('ETH Price on Bitstamp')

ax1.text(0.1,0.9,'Correlation Prices: ' + str("{:0.3f}".format(corr_gem['close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax1.transAxes)
ax1.text(0.1,0.85,'Correlation Returns: ' + str("{:0.3f}".format(corret_gem['close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax1.transAxes)
ax1.text(0.1,0.80,'Abs Div. (bps): ' + str("{:0.3f}".format(absdiv_gem[0]*100)) , horizontalalignment='left', verticalalignment='center', transform=ax1.transAxes)

ax2.text(0.1,0.9,'Correlation Prices: ' + str("{:0.3f}".format(corr_krak['close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax2.transAxes)
ax2.text(0.1,0.85,'Correlation Returns: ' + str("{:0.3f}".format(corret_krak['close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax2.transAxes)
ax2.text(0.1,0.8,'Abs Div. (bps): ' + str("{:0.3f}".format(absdiv_krak[0]*100)) , horizontalalignment='left', verticalalignment='center', transform=ax2.transAxes)

ax3.text(0.1,0.9,'Correlation Prices: ' + str("{:0.3f}".format(corr_bit['close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax3.transAxes)
ax3.text(0.1,0.85,'Correlation Returns: ' + str("{:0.3f}".format(corret_bit['close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax3.transAxes)
ax3.text(0.1,0.80,'Abs Div. (bps): ' + str("{:0.3f}".format(absdiv_bit[0]*100)) , horizontalalignment='left', verticalalignment='center', transform=ax3.transAxes)

fig.suptitle('Spot ETH Prices across U.S. Exchanges')






