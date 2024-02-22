##############################################################################################
# Lorenz Curve of BTC and ETH Ownership on Coinbase Platform. 
# Feb 2024
# Coinbase Policy
# Coinbase Response Letter to the Grayscale ETH ETF Application
##############################################################################################

#Importing Libraries
import cb_databricks.snowflake
import pandas as pd
import numpy as np
from pyspark.sql.functions import when
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, floor
import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as F
import matplotlib.pyplot as plt


##############################################################################################
#Import ETH and BTC data

sql = """SELECT ds , user_id, SUM(day_end_balance_usd) as balance
          FROM accounting.auc_daily_balances_pro_consumer_custody
          WHERE DAY(DS) = 31 AND MONTH(DS) = 12 AND YEAR(DS) = 2023 AND CURRENCY = 'ETH'
          GROUP BY ds, USER_ID
          ORDER BY balance DESC

          """
df_eth = cb_databricks.snowflake.load_as_spark_dataframe(sql, use_lowercase_column_names=True)
df_eth = df_eth.filter('balance > 1000')
display(df_eth)

sql = """SELECT ds , user_id, SUM(day_end_balance_usd) as balance
          FROM accounting.auc_daily_balances_pro_consumer_custody
          WHERE DAY(DS) = 31 AND MONTH(DS) = 12 AND YEAR(DS) = 2023 AND CURRENCY = 'BTC' 
          GROUP BY ds, USER_ID
          ORDER BY balance DESC
          """
df_btc = cb_databricks.snowflake.load_as_spark_dataframe(sql, use_lowercase_column_names=True)
df_btc = df_btc.filter('balance > 1000')
display(df_btc)

##############################################################################################
#Compute ETH Concentration

df_balance_eth = df_eth
#define window for calculating cumulative sum
my_window = (Window.orderBy(col('balance').desc()).rowsBetween(Window.unboundedPreceding, 0))

#create new DataFrame that contains cumulative sales column
totbal = df_balance_eth.agg({'balance': 'sum'}).collect()[0]["sum(balance)"]
display(totbal)
nrow = df_balance_eth.count()
display(nrow)
df3 = spark.range(1)
df3 = df3.select(floor(lit(nrow/1000)))
split = df3.head()[0]
display(split)
df_balance_eth = df_balance_eth.withColumn("cum_token", F.sum('balance').over(my_window) / totbal)
df_balance_eth = df_balance_eth.withColumn('cum_users', row_number().over(my_window) / nrow)
df_balance_eth2 = df_balance_eth.withColumn("index", row_number().over(my_window) ).filter((col('index')  % split == 0) | (col('index') == 1))
df_balance_eth2 = df_balance_eth2.withColumn('token', lit('eth'))
display(df_balance_eth2)


##############################################################################################
#Compute BTC Concentration and Combine with ETH

df_balance_btc = df_btc

#create new DataFrame that contains cumulative sales column
totbal = df_balance_btc.agg({'balance': 'sum'}).collect()[0]["sum(balance)"]
display(totbal)
nrow = df_balance_btc.count()
display(nrow)
df3 = spark.range(1)
df3 = df3.select(floor(lit(nrow/1000)))
split = df3.head()[0]
display(split)
df_balance_btc = df_balance_btc.withColumn("cum_token", F.sum('balance').over(my_window) / totbal)
df_balance_btc = df_balance_btc.withColumn('cum_users', row_number().over(my_window) / nrow)
df_balance_btc2 = df_balance_btc.withColumn("index", row_number().over(my_window) ).filter((col('index') % split == 0) | (col('index') ==1))
df_balance_btc2 = df_balance_btc2.withColumn('token', lit('btc'))

df = df_balance_btc2.union(df_balance_eth2)
display(df)


##############################################################################################
#Plot Lorenz Curves

pdf = df.toPandas()
fig, ax = plt.subplots()
pdf.loc[pdf['token'] == 'btc'].info()
pdf['cum_token'] = pd.to_numeric(pdf['cum_token'], downcast = 'float')
pdf.loc[pdf['token'] == 'btc'].plot(x = 'cum_users', y = 'cum_token', ax = ax)

pdf.loc[pdf['token'] == 'eth'].plot(x = 'cum_users', y = 'cum_token', ax = ax)
ax.legend(["BTC", "ETH"])
ax.set_title('Ownership Concentration - All Coinbase Users')
ax.set_xlabel("Cumulative Users Perc.")
ax.set_ylabel("Cumulative Holdings Perc.")

plt.show()
