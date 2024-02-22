##############################################################################################
# Non US ETH ETF Vs Coinbase Spot 
# Feb 2024
# Coinbase Policy
# Coinbase Response Letter to the Grayscale ETH ETF Application
##############################################################################################

##############################################################################################
# Load Up Coinbase Spot Prices in EURO

sql = """ select *, volume*close as volume_dollar
from ANALYTICS.EXCHANGE.HISTORIC_MARKET_DATA
where quote_currency = 'EUR' and BASE_CURRENCY = 'ETH' and GRANULARITY = 'ONE_HOUR' and start_date > '2022-12-31' and source = 'EXCHANGE'
order by start_date """

df_eur = cb_databricks.snowflake.load_as_spark_dataframe(sql, use_lowercase_column_names=True)
df_eur = df_eur.select('start', 'close')
df_eur = df_eur.withColumnRenamed("close", "close_coinbase_eur")
df_eur = df_eur.withColumnRenamed("start", "Date")
pdf_eur = df_eur.toPandas()

sql = """ select *, volume*close as volume_dollar
from ANALYTICS.EXCHANGE.HISTORIC_MARKET_DATA
where quote_currency = 'CAD' and BASE_CURRENCY = 'ETH' and GRANULARITY = 'ONE_HOUR' and start_date > '2022-12-31' and source = 'ROBUST'
order by start_date """

df_cad = cb_databricks.snowflake.load_as_spark_dataframe(sql, use_lowercase_column_names=True)
df_cad = df_cad.select('start', 'close')
df_cad = df_cad.withColumnRenamed("close", "close_coinbase_cad")
df_cad = df_cad.withColumnRenamed("start", "Date")

pdf_cad = df_cad.toPandas()
print(pdf_eur)
print(pdf_cad)


##############################################################################################
# Load Up ETH ETF Data

etf_ethw = pd.read_csv('/dbfs/FileStore/ETH_ETF___AETH_FP.csv')
etf_ethw['Date'] = etf_ethw['Date'].astype('datetime64')
etf_ethw_merge = etf_ethw.merge(pdf_eur, on = 'Date', how = 'inner')
etf_ethw_merge['close_coinbase_eur'] = etf_ethw_merge['close_coinbase_eur'].astype(float)
etf_ethw_merge.sort_values(by = ['Date'], inplace = True)
ethw_vol = etf_ethw_merge['Volume'] * etf_ethw_merge['Close']
ethw_vol = ethw_vol.mean()


etf_ceth = pd.read_csv('/dbfs/FileStore/ETH_ETF___CETH.csv')
etf_ceth['Date'] = etf_ceth['Date'].astype('datetime64')
etf_ceth_merge = etf_ceth.merge(pdf_eur, on = 'Date', how = 'inner')
etf_ceth_merge['close_coinbase_eur'] = etf_ceth_merge['close_coinbase_eur'].astype(float)
etf_ceth_merge.sort_values(by = ['Date'], inplace = True)
ceth_vol = etf_ceth_merge['Volume'] * etf_ceth_merge['Close']
ceth_vol = ceth_vol.mean()

etf_veth = pd.read_csv('/dbfs/FileStore/ETH_ETF___VETH.csv')
etf_veth['Date'] = etf_veth['Date'].astype('datetime64')
etf_veth_merge = etf_veth.merge(pdf_eur, on = 'Date', how = 'inner')
etf_veth_merge['close_coinbase_eur'] = etf_veth_merge['close_coinbase_eur'].astype(float)
etf_veth_merge.sort_values(by = ['Date'], inplace = True)
veth_vol = etf_veth_merge['Volume'] * etf_veth_merge['Close']
veth_vol = veth_vol.mean()

##############################################################################################
# Compute Correlation

corr_ethw = etf_ethw_merge[['Close', 'close_coinbase_eur']].corr()
corr_ceth = etf_ceth_merge[['Close', 'close_coinbase_eur']].corr()
corr_veth = etf_veth_merge[['Close', 'close_coinbase_eur']].corr()

corret_ethw = etf_ethw_merge[['Close', 'close_coinbase_eur']].pct_change().corr()
corret_ceth = etf_ceth_merge[['Close', 'close_coinbase_eur']].pct_change().corr()
corret_veth = etf_veth_merge[['Close', 'close_coinbase_eur']].pct_change().corr()


##############################################################################################
# Plot Scatterplot

plt.rcParams.update({'font.size': 20})
print(corr_ethw)

fig, (ax1, ax2, ax3) = plt.subplots(1,3,figsize = (33,11))
ax1.scatter(etf_ethw_merge['close_coinbase_eur'].to_numpy(), etf_ethw_merge['Close'].to_numpy(), c='DarkBlue' )
ax1.set_title('AETH:FP ETF vs Spot Coinbase')
ax1.set_xlabel('ETH Spot Price (EUR) on Coinbase')
ax1.set_ylabel('AETH:FP ETF Price (EUR)')

ax2.scatter(etf_ceth_merge['close_coinbase_eur'].to_numpy(), etf_ceth_merge['Close'].to_numpy(), c='DarkBlue' )
ax2.set_title('CETH ETF vs Spot Coinbase')
ax2.set_xlabel('ETH Spot Price (EUR) on Coinbase')
ax2.set_ylabel('CETH ETF Price (EUR)')

ax3.scatter(etf_veth_merge['close_coinbase_eur'].to_numpy(), etf_veth_merge['Close'].to_numpy(), c='DarkBlue' )
ax3.set_title('VETH ETF vs Spot Coinbase')
ax3.set_xlabel('ETH Spot Price (EUR) on Coinbase')
ax3.set_ylabel('VETH ETF Price (EUR)')

ax1.text(0.1,0.9,'Corr. Prices: ' + str("{:0.3f}".format(corr_ethw['Close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax1.transAxes)
ax1.text(0.1,0.85,'Corr. Hourly Returns: ' + str("{:0.3f}".format(corret_ethw['Close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax1.transAxes)
ax1.text(0.1,0.8,'Avg Hourly Volume (EUR): ' + str("{:0.0f}".format(ethw_vol)) , horizontalalignment='left', verticalalignment='center', transform=ax1.transAxes)



ax2.text(0.1,0.9,'Corr. Prices: ' + str("{:0.3f}".format(corr_ceth['Close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax2.transAxes)
ax2.text(0.1,0.85,'Corr. Hourly Returns: ' + str("{:0.3f}".format(corret_ceth['Close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax2.transAxes)
ax2.text(0.1,0.8,'Avg Hourly Volume (EUR): ' + str("{:0.0f}".format(ceth_vol)) , horizontalalignment='left', verticalalignment='center', transform=ax2.transAxes)

ax3.text(0.1,0.9,'Corr. Prices: ' + str("{:0.3f}".format(corr_veth['Close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax3.transAxes)
ax3.text(0.1,0.85,'Corr. Hourly Returns: ' + str("{:0.3f}".format(corret_veth['Close'][1]*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax3.transAxes)
ax3.text(0.1,0.8,'Avg Hourly Volume (EUR): ' + str("{:0.0f}".format(veth_vol)) , horizontalalignment='left', verticalalignment='center', transform=ax3.transAxes)

fig.suptitle('ETH ETF vs ETH Spot Prices')
