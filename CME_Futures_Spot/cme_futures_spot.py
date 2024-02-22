##############################################################################################
# CME Futures Vs Spot ETH and BTC  
# Feb 2024
# Coinbase Policy
# Coinbase Response Letter to the Grayscale ETH ETF Application
##############################################################################################

##############################################################################################
# CME data comes from Databento.com APIs

client = db.Historical("INSERT_DATABENTO_API_KEY_HERE")

#BTC
data = client.timeseries.get_range(
    start="2017-05-21",
    end="2024-02-01",
    dataset="GLBX.MDP3",
    symbols=["BTC.c.0"],
    stype_in="continuous",
    stype_out="instrument_id",
    schema="ohlcv-1m",
)

df = data.to_df()
df.reset_index(inplace = True)
df['token'] = df['symbol'].str[:3]
df.sort_values(['token', 'ts_event', 'symbol'], ascending = True, inplace = True)
df['date'] = pd.to_datetime(df['ts_event'])
df.to_csv('/content/drive/MyDrive/CME Data/CMEData_c0_btc.csv')

#ETH
data = client.timeseries.get_range(
    start="2017-05-21",
    end="2024-02-01",
    dataset="GLBX.MDP3",
    symbols=["ETH.c.0"],
    stype_in="continuous",
    stype_out="instrument_id",
    schema="ohlcv-1m",
)

df = data.to_df()
df.reset_index(inplace = True)
df['token'] = df['symbol'].str[:3]
df.sort_values(['token', 'ts_event', 'symbol'], ascending = True, inplace = True)
df['date'] = pd.to_datetime(df['ts_event'])
df.to_csv('/content/drive/MyDrive/CME Data/CMEData_c0_eth.csv')


##############################################################################################
# Install packages

from pyspark.sql.window import Window
from pyspark.sql.functions import col,desc,lag,window,corr,year,month,collect_list,explode,expr,date_sub,from_utc_timestamp, to_utc_timestamp,min, max,lit, hour, minute, concat, first
from pyspark.sql import functions as F
from pyspark.sql import Window
import matplotlib.pyplot as plt
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from datetime import datetime, timedelta
import cb_databricks.snowflake
!pip install databento
import databento as db
import pandas as pd
import datetime
import time


##############################################################################################
# Load up and Clean Futures data

# read cme data and trading hours. Do required conversions
cme_btc = spark.read.csv('/FileStore/CMEData_c0_btc.csv', header=True, inferSchema=True)
cme_btc = cme_btc.filter((col("date") >= '2021-03-01') & (col("date") <= '2024-01-31'))

# read cme data and trading hours. Do required conversions
cme_eth = spark.read.csv('/FileStore/CMEData_c0_eth.csv', header=True, inferSchema=True)
cme_eth = cme_eth.filter((col("date") >= '2021-03-01') & (col("date") <= '2024-01-31'))

# Order the DataFrame cme_btc in ascending order
cme_btc = cme_btc.orderBy("date")
cme_eth = cme_eth.orderBy("date")
windowSpec = Window.orderBy("date")

# DataFrame with five-minute intervals
cme_btc_5m = cme_btc.filter(minute('date') % 5 == 4).select("date","close")
cme_btc_5m = cme_btc_5m.withColumnRenamed('date','date_old')
cme_btc_5m = cme_btc_5m.withColumn('date' , col('date_old') - F.expr('INTERVAL 4 MINUTES'))
cme_btc_1h = cme_btc.filter(minute('date') == 59).select("date","close")
cme_btc_1h = cme_btc_1h.withColumnRenamed('date','date_old')
cme_btc_1h = cme_btc_1h.withColumn('date' , col('date_old') - F.expr('INTERVAL 59 MINUTES'))

cme_eth_5m = cme_eth.filter(minute('date') % 5 == 4).select("date","close")
cme_eth_5m = cme_eth_5m.withColumnRenamed('date','date_old')
cme_eth_5m = cme_eth_5m.withColumn('date' , col('date_old') - F.expr('INTERVAL 4 MINUTES'))
cme_eth_1h = cme_eth.filter(minute('date') == 59 ).select("date","close")
cme_eth_1h = cme_eth_1h.withColumnRenamed('date','date_old')
cme_eth_1h = cme_eth_1h.withColumn('date' , col('date_old') - F.expr('INTERVAL 59 MINUTES'))


##############################################################################################
# Load up and Clean Spot data (from coinbase)

sql = """ SELECT PRODUCT_GRANULARITY_SOURCE, "START" AS TIME, CLOSE AS CLOSE_SPOT
FROM ANALYTICS.EXCHANGE.HISTORIC_MARKET_DATA
WHERE PRODUCT_GRANULARITY_SOURCE IN ('BTC-USD::ONE_HOUR::EXCHANGE','BTC-USD::FIVE_MINUTE::EXCHANGE','BTC-USD::ONE_MINUTE::EXCHANGE','ETH-USD::ONE_HOUR::EXCHANGE','ETH-USD::FIVE_MINUTE::EXCHANGE','ETH-USD::ONE_MINUTE::EXCHANGE') AND  START_DATE > '2021-02-28' AND START_DATE < '2024-02-01' """

df = cb_databricks.snowflake.load_as_spark_dataframe(sql, use_lowercase_column_names=True)


##############################################################################################
# Merge Spot and Future ETH, and compute Full sample correlation

# select correct granularity
btc_1m = df.filter(col("PRODUCT_GRANULARITY_SOURCE") == "BTC-USD::ONE_MINUTE::EXCHANGE").drop('PRODUCT_GRANULARITY_SOURCE')
btc_5m = df.filter(col("PRODUCT_GRANULARITY_SOURCE") == "BTC-USD::FIVE_MINUTE::EXCHANGE").drop('PRODUCT_GRANULARITY_SOURCE')
btc_1h = df.filter(col("PRODUCT_GRANULARITY_SOURCE") == "BTC-USD::ONE_HOUR::EXCHANGE").drop('PRODUCT_GRANULARITY_SOURCE')

# Perform a left join on datetime and start columns
btc_merged_1m = cme_btc.join(btc_1m, cme_btc['date'] == btc_1m['time'], 'inner').orderBy(col("date"))
btc_merged_5m = cme_btc_5m.join(btc_5m, cme_btc_5m['date'] == btc_5m['time'], 'inner').orderBy(col("date"))
btc_merged_1h = cme_btc_1h.join(btc_1h, cme_btc_1h['date'] == btc_1h['time'], 'inner').orderBy(col("date"))

w = Window.orderBy(col("date"))

# Compute returns
btc_merged_1m = btc_merged_1m.withColumn("spot_return", (col("close_spot") - lag("close_spot", 1).over(w)) / lag("close_spot", 1).over(w))
btc_merged_1m = btc_merged_1m.withColumn("cme_return", (col("close") - lag("close", 1).over(w)) / lag("close", 1).over(w))
btc_merged_5m = btc_merged_5m.withColumn("spot_return", (col("close_spot") - lag("close_spot", 1).over(w)) / lag("close_spot", 1).over(w))
btc_merged_5m = btc_merged_5m.withColumn("cme_return", (col("close") - lag("close", 1).over(w)) / lag("close", 1).over(w))
btc_merged_1h = btc_merged_1h.withColumn("spot_return", (col("close_spot") - lag("close_spot", 1).over(w)) / lag("close_spot", 1).over(w))
btc_merged_1h = btc_merged_1h.withColumn("cme_return", (col("close") - lag("close", 1).over(w)) / lag("close", 1).over(w))

btc_merged_1m_corr = btc_merged_1m.stat.corr("spot_return", "cme_return")
print("Correlation of minutely returns between BTC SPOT AND FUTURES:", btc_merged_1m_corr)
btc_merged_5m_corr = btc_merged_5m.stat.corr("spot_return", "cme_return")
print("Correlation of 5 minutely returns between BTC SPOT AND FUTURES:", btc_merged_5m_corr)
btc_merged_1h_corr = btc_merged_1h.stat.corr("spot_return", "cme_return")
print("Correlation of hourly returns between BTC SPOT AND FUTURES:", btc_merged_1h_corr)



##############################################################################################
# Compute Rolling Correlation

# Define a window spec for the three-month rolling correlation
one_min_window = Window.orderBy(col("date")).rowsBetween(-107640, 0) 
five_min_window = Window.orderBy(col("date")).rowsBetween(-21528, 0) 
one_hour_window = Window.orderBy(col("date")).rowsBetween(-1794, 0) 

# Compute the correlation
one_min_rol = btc_merged_1m.select(
    btc_merged_1m["date"],
    btc_merged_1m["spot_return"],
    btc_merged_1m["cme_return"]
).withColumn(
    "rolling_correlation",
    F.corr("spot_return", "cme_return").over(one_min_window)
)
one_min_rol = spark.createDataFrame(one_min_rol.tail(one_min_rol.count()-107640), one_min_rol.schema)

# Compute the correlation
five_min_rol = btc_merged_5m.select(
    btc_merged_5m["date"],
    btc_merged_5m["spot_return"],
    btc_merged_5m["cme_return"]
).withColumn(
    "rolling_correlation",
    F.corr("spot_return", "cme_return").over(five_min_window)
)
five_min_rol = spark.createDataFrame(five_min_rol.tail(five_min_rol.count()-21528), five_min_rol.schema)

# Compute the correlation
one_hour_rol = btc_merged_1h.select(
    btc_merged_1h["date"],
    btc_merged_1h["spot_return"],
    btc_merged_1h["cme_return"]
).withColumn(
    "rolling_correlation",
    F.corr("spot_return", "cme_return").over(one_hour_window)
)
one_hour_rol = spark.createDataFrame(one_hour_rol.tail(one_hour_rol.count()-1794), one_hour_rol.schema)

# Get the minimum and maximum correlations
one_min_minimum_rol = one_min_rol.agg({"rolling_correlation": "min"}).collect()[0]
one_min_maximum_rol = one_min_rol.agg({"rolling_correlation": "max"}).collect()[0]

print("Minimum minutely BTC Spot Futures correlation:", one_min_minimum_rol)
print("Maximum minutely BTC Spot Futures correlation:", one_min_maximum_rol)

five_min_minimum_rol = five_min_rol.agg({"rolling_correlation": "min"}).collect()[0]
five_min_maximum_rol = five_min_rol.agg({"rolling_correlation": "max"}).collect()[0]

print("Minimum five minute BTC Spot Futures correlation:", five_min_minimum_rol)
print("Maximum five minute BTC Spot Futures correlation:", five_min_maximum_rol)

one_hour_minimum_rol = one_hour_rol.agg({"rolling_correlation": "min"}).collect()[0]
one_hour_maximum_rol = one_hour_rol.agg({"rolling_correlation": "max"}).collect()[0]

print("Minimum hourly BTC Spot Futures correlation:", one_hour_minimum_rol)
print("Maximum hourly BTC Spot Futures correlation:", one_hour_maximum_rol)


##############################################################################################
# ETH Full Sample Correlation

# select correct granularity
eth_1m = df.filter(col("PRODUCT_GRANULARITY_SOURCE") == "ETH-USD::ONE_MINUTE::EXCHANGE").drop('PRODUCT_GRANULARITY_SOURCE')
eth_5m = df.filter(col("PRODUCT_GRANULARITY_SOURCE") == "ETH-USD::FIVE_MINUTE::EXCHANGE").drop('PRODUCT_GRANULARITY_SOURCE')
eth_1h = df.filter(col("PRODUCT_GRANULARITY_SOURCE") == "ETH-USD::ONE_HOUR::EXCHANGE").drop('PRODUCT_GRANULARITY_SOURCE')


# Perform a left join on datetime and start columns
eth_merged_1m = cme_eth.join(eth_1m, cme_eth['date'] == eth_1m['time'], 'inner').orderBy(col("date"))
eth_merged_5m = cme_eth_5m.join(eth_5m, cme_eth_5m['date'] == eth_5m['time'], 'inner').orderBy(col("date"))
eth_merged_1h = cme_eth_1h.join(eth_1h, cme_eth_1h['date'] == eth_1h['time'], 'inner').orderBy(col("date"))


w = Window.orderBy(col("date"))


# Compute returns
eth_merged_1m = eth_merged_1m.withColumn("spot_return", (col("close_spot") - lag("close_spot", 1).over(w)) / lag("close_spot", 1).over(w))
eth_merged_1m = eth_merged_1m.withColumn("cme_return", (col("close") - lag("close", 1).over(w)) / lag("close", 1).over(w))
eth_merged_5m = eth_merged_5m.withColumn("spot_return", (col("close_spot") - lag("close_spot", 1).over(w)) / lag("close_spot", 1).over(w))
eth_merged_5m = eth_merged_5m.withColumn("cme_return", (col("close") - lag("close", 1).over(w)) / lag("close", 1).over(w))
eth_merged_1h = eth_merged_1h.withColumn("spot_return", (col("close_spot") - lag("close_spot", 1).over(w)) / lag("close_spot", 1).over(w))
eth_merged_1h = eth_merged_1h.withColumn("cme_return", (col("close") - lag("close", 1).over(w)) / lag("close", 1).over(w))

#eth_merged_1h['close_spot']=eth_merged_1h['close_spot'].astype(float)
display(eth_merged_1m)
display(eth_merged_5m)
display(eth_merged_1h)


eth_merged_1m.summary().show()
eth_merged_5m.summary().show()
eth_merged_1h.summary().show()


eth_merged_1m_corr = eth_merged_1m.stat.corr("spot_return", "cme_return")
print("Correlation of minutely returns between eth SPOT AND FUTURES:", eth_merged_1m_corr)
eth_merged_5m_corr = eth_merged_5m.stat.corr("spot_return", "cme_return")
print("Correlation of 5 minutely returns between eth SPOT AND FUTURES:", eth_merged_5m_corr)
eth_merged_1h_corr = eth_merged_1h.stat.corr("spot_return", "cme_return")
print("Correlation of hourly returns between eth SPOT AND FUTURES:", eth_merged_1h_corr)


##############################################################################################
# Plot ETH Futures vs spot prices


plt.rcParams.update({'font.size': 20})
eth_merged_1h_corrprice = eth_merged_1h.stat.corr("close_spot", "close")

df_eth = eth_merged_1h.toPandas() 
df_eth['close_spot'] = df_eth['close_spot'].astype(float)
df_eth = df_eth.loc[df_eth['date']>'2022-12-31']
fig,ax = plt.subplots(figsize = (20,10))
df_eth[['date','close', 'close_spot']].set_index('date').plot(ax=ax, legend=False)
ax.legend(['ETH Futures Price', 'ETH Spot Coinbase Price'])
ax.set_ylabel('ETH Price ($)')
ax.set_xlabel('Date')
ax.text(0.5,0.75,'Correlation Prices:    ' + str("{:0.3f}".format(eth_merged_1h_corrprice*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax.transAxes)
ax.text(0.5,0.7,'Correlation Returns: ' + str("{:0.3f}".format(eth_merged_1h_corr*100) + '%') , horizontalalignment='left', verticalalignment='center', transform=ax.transAxes)
fig.suptitle('Spot Vs Futures ETH Prices')

fig.show()

##############################################################################################
# ETH Rolling Correlation

# Define a window spec for the three-month rolling correlation
one_min_window = Window.orderBy(col("date")).rowsBetween(-107640, 0)
five_min_window = Window.orderBy(col("date")).rowsBetween(-21528, 0)
one_hour_window = Window.orderBy(col("date")).rowsBetween(-1794, 0)


# Compute the correlation
one_min_rol = eth_merged_1m.select(
   eth_merged_1m["date"],
   eth_merged_1m["spot_return"],
   eth_merged_1m["cme_return"]
).withColumn(
   "rolling_correlation",
   F.corr("spot_return", "cme_return").over(one_min_window)
)
one_min_rol = spark.createDataFrame(one_min_rol.tail(one_min_rol.count()-107640), one_min_rol.schema)


# Compute the correlation
five_min_rol = eth_merged_5m.select(
   eth_merged_5m["date"],
   eth_merged_5m["spot_return"],
   eth_merged_5m["cme_return"]
).withColumn(
   "rolling_correlation",
   F.corr("spot_return", "cme_return").over(five_min_window)
)
five_min_rol = spark.createDataFrame(five_min_rol.tail(five_min_rol.count()-21528), five_min_rol.schema)


# Compute the correlation
one_hour_rol = eth_merged_1h.select(
   eth_merged_1h["date"],
   eth_merged_1h["spot_return"],
   eth_merged_1h["cme_return"]
).withColumn(
   "rolling_correlation",
   F.corr("spot_return", "cme_return").over(one_hour_window)
)
one_hour_rol = spark.createDataFrame(one_hour_rol.tail(one_hour_rol.count()-1794), one_hour_rol.schema)


# Get the minimum and maximum correlations
one_min_minimum_rol = one_min_rol.agg({"rolling_correlation": "min"}).collect()[0]
one_min_maximum_rol = one_min_rol.agg({"rolling_correlation": "max"}).collect()[0]


print("Minimum minutely eth Spot Futures correlation:", one_min_minimum_rol)
print("Maximum minutely eth Spot Futures correlation:", one_min_maximum_rol)


five_min_minimum_rol = five_min_rol.agg({"rolling_correlation": "min"}).collect()[0]
five_min_maximum_rol = five_min_rol.agg({"rolling_correlation": "max"}).collect()[0]


print("Minimum five minute eth Spot Futures correlation:", five_min_minimum_rol)
print("Maximum five minute eth Spot Futures correlation:", five_min_maximum_rol)


one_hour_minimum_rol = one_hour_rol.agg({"rolling_correlation": "min"}).collect()[0]
one_hour_maximum_rol = one_hour_rol.agg({"rolling_correlation": "max"}).collect()[0]


print("Minimum hourly eth Spot Futures correlation:", one_hour_minimum_rol)
print("Maximum hourly eth Spot Futures correlation:", one_hour_maximum_rol)






