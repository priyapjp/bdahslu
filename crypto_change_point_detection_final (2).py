#!/usr/bin/env python
# coding: utf-8

# In[41]:


import pandas as pd
import numpy as np
import datetime
import os
import ntpath




# INPUT - Folder path with the clean files
directory = 'C:\\Users\\florian.bozzone\\Desktop\\Bitfinex_Clean'

# INPUT - Folder path where csv should be saved to
save_path = 'C:\\Users\\florian.bozzone\\Desktop\\Analysis'

# PARAMETER - Window size for moving average
ma_window=120 

# PARAMETER - PRICE threshold for detecting changepoints
price_threshold=15

# PARAMETER -  Volume threshold for detecting changepoints
volume_threshold=300

# PARAMETER - Unix Time ranges - we look for two ranges
range1= 0 #range 1 min
range2= 0 #range 1 max

range3=0.5 #range 2 min
range4=0.5 #range 2 max





#Create empty Dataframe
alerts = pd.DataFrame()

#loop over all files in directory
for entry in os.scandir(directory):
    if (entry.path.endswith(".csv")
            or entry.path.endswith(".png")) and entry.is_file():
        
        data = pd.read_csv(entry.path)
        
        #Define exchange name and coin name from folder path
        filename = ntpath.basename(entry.path)
        fn = filename.split('_')
        exchange = fn[0]
        coin = fn[1] 
        binance = fn[1] + "-" + fn[2]
        
        #Add exchange name and coin name to dataframe
        data.insert(1,column='Exchange', value=exchange)
        data.insert(2,column='Coin',value=coin)
        data.insert(3,column="Binance",value=binance)
        
        #Convert Timestamp to a date format - is string 
        data['Timestamp']=pd.to_datetime(data['Timestamp'])
        
        #Add unix epoche and unix time indicator
        data['Unix_Time']=data['Timestamp'].view('int64')
        data['Unix_Indicator']=data['Unix_Time']/1000000000 % 3600 / 3600
        
        #Add moving average over closing price and calculate delta to closing price
        data['MA_Close']=data.Close.rolling(window=ma_window).mean()
        data['Delta_Price_%']=(abs(data.MA_Close - data.Close))/data.MA_Close*100
        
        #Add moving average over vloume and calculate delta to volume
        data['MA_Volume']=data.Volume.rolling(window=ma_window).mean()
        data['Delta_Volume_%'] = (abs(data.MA_Volume - data.Volume))/data.MA_Volume*100
        
        #Identify subset of data - changepoint detection
        delta_data = data.loc[(data['Delta_Price_%']>=price_threshold) & (data['Delta_Volume_%']>=volume_threshold) & (data['Unix_Indicator'].between(range1,range2)) | 
                              (data['Delta_Price_%']>=price_threshold) & (data['Delta_Volume_%']>=volume_threshold) & (data['Unix_Indicator'].between(range3,range4)) ]
        
        #Add the identified data to the overall dataframe
        alerts = alerts.append(delta_data)

#define folder path incl. filename
output_path = save_path + '\\' + exchange + '_'+ 'alerts' + '_' + 'window' + '-' + str(ma_window) + '_' + 'price' + '-' + str(price_threshold) + '_' + 'volume' + '-' + str(volume_threshold) + '_' + 'findings' + '_' + str(len(alerts)) + '.csv' 

#create and save CSV
alerts.to_csv(output_path, index = False)

#Count Findings
print(exchange)
len(alerts)


# In[ ]:





# In[ ]:




