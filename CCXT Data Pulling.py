#!/usr/bin/env python
# coding: utf-8

# In[1]:


import ccxt
import pandas as pd
import statistics as stat
from math import pi
import time
import random
import os

#plotting
from bokeh.plotting import figure, show, output_file
from bokeh.io import output_notebook
from bokeh.models import Axis
from bokeh.layouts import gridplot
from bokeh.plotting import figure


# In[2]:


def create_ohlcv_df(data):
    header = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(data, columns=header)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'],unit='ms',origin='unix') # convert timestamp to datetime
    return df


# In[3]:


# pulls "n_candles" number of candles with "c_size" candlesticks from a given start date
# for all symbol pairs on a given exchange and stores them in a pandas dataframe, 
# then saves them in csv's in the specified folder path
# returns a list with the symbols it could not fetch

# -- EXAMPLE INPUT --
# exchange = 'bittrex'
# from_date = '2018-04-20 00:00:00'
# n_candles = 480
# c_size = '1h'
# f_path = '../data'

def pull_data(exchange,from_date,n_candles,c_size,f_path,skip=False):
    count = 1
    msec = 1000
    hold = 5  # waiting time between retry
    
    missing_symbols = []

    # -- create a folder --
    newpath = f_path+'/'+exchange+'/'
    if not os.path.exists(newpath):
        os.makedirs(newpath)

    # -- load exchange --
    exc_instance = getattr(ccxt,exchange)()
    exc_instance.load_markets()
    from_timestamp = exc_instance.parse8601(from_date)
    
    # -- pull ohlcv --
    for symbol in exc_instance.symbols:
        for attempt in range(1): # 1 attempts max
            try:
                print('Pulling:',exchange,':',symbol, '[{}/{}]'.format(count,len(exc_instance.symbols)))
                data = exc_instance.fetch_ohlcv(symbol, c_size, from_timestamp, n_candles)
                
                # if < n_candles returned, skip this pair
                if len(data) < n_candles and skip is True:
                    continue
                
                # -- create DF --
                df = create_ohlcv_df(data)
                
                # Skript changed by Florian - START
                print(len(data)) 
                # Skript changed by Florian - END

                # -- save CSV --
                symbol = symbol.replace("/","-")
                filename = newpath+'{}_{}_[{}]-TO-[{}].csv'.format(exchange, symbol, df['Timestamp'].iloc[0], df['Timestamp'].iloc[-1])        
                df.to_csv(filename)
                
            except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout, IndexError) as error:
                print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
                time.sleep(hold)
            else: # if no error, proceed to next iteration
                break
        else: # we failed all attempts
            print('All attempts failed, skipping:',symbol)
            missing_symbols.append(symbol)
            continue
            
        count += 1   
        # -- wait for rate limit --
        time.sleep((exc_instance.rateLimit / msec) + 3) # rate limit +5 seconds to just to be safe

    # print out any symbols we could not obtain
    if len(missing_symbols) is not 0:
        print('Unable to obtain:',missing_symbols)
    
    
    return missing_symbols


# In[4]:


# gets the symbols from a given "exchange", filtering out any symbol pairs provided in "without", and stores in a df
# additionally, if set to save and given a filepath, will save as .csv

# f_path = '../symbols'
def symbol_list_df(exchange, without=[], save=False, f_path=''):

    # -- load market --
    exc_instance = getattr(ccxt, exchange)()
    exc_instance.load_markets()
    
    # -- filter list --
    symbol_list = [s for s in exc_instance.symbols if s not in without]
    
    # -- save --
    if save is True:
      
        # -- create a folder --
        newpath = f_path+'/'
        if not os.path.exists(newpath):
            os.makedirs(newpath)
        
        # -- create df --
        header = ['Symbols']
        df = pd.DataFrame(symbol_list, columns=header)
        filename = newpath+'{}_symbols.csv'.format(exchange)        
        df.to_csv(filename)
            
    return symbol_list


# In[8]:


from_date = '2020-01-03 00:00:00'
#exchanges = ['acx','bitfinex','bittrex','btcalpha','digifinex','stex','tidebit']
exchanges = ['binance']

for e in exchanges:
    #pull_data(e,from_date,5,'1m','/home/jovyan/CCXT DATA')
    pull_data(e,from_date,720,'1m','C:\\Users\\florian.bozzone\\Desktop\\CCXT\\binance')

