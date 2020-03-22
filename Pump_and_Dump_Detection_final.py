#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from tkinter import *
from PIL import ImageTk,Image
from tkinter import filedialog
import pandas as pd
import numpy as np
import datetime
import os
import ntpath
from pathlib import Path
from datetime import datetime, timedelta
import plotly.graph_objects as go

root = Tk()

root.title('Disraptors - Change Point Detection')
#root.iconbitmap('C:/Users/florian.bozzone/Downloads/disraptor.ico')

Label(root, text="Clean Data Folder").grid(row=0)
Label(root, text="Saving Folder").grid(row=1)
Label(root, text="Moving Average Window").grid(row=2)
Label(root, text="Price Threshold").grid(row=3)
Label(root, text="Volume Threshold").grid(row=4)
Label(root, text="Unix Time Indicator Window 1").grid(row=5)
Label(root, text="To").grid(row=5, column=2)
Label(root, text="Unix Time Indicator Window 2").grid(row=6)
Label(root, text="To").grid(row=6, column=2)
Label(root, text="Select Row").grid(row=9)
Label(root, text="Define Candlesize").grid(row=10)

e1 = Entry(root)
e1.grid(row=2, column=1)
e1.insert(0,"60")

e2 = Entry(root)
e2.grid(row=3, column=1)
e2.insert(0,"15")

e3 = Entry(root)
e3.grid(row=4, column=1)
e3.insert(0,"500")

e4 = Entry(root)
e4.grid(row=5, column=1)
e4.insert(0,"0")

e5 = Entry(root)
e5.grid(row=5, column=3)
e5.insert(0,"0")

e6 = Entry(root)
e6.grid(row=6, column=1)
e6.insert(0,"0.5")

e7 = Entry(root)
e7.grid(row=6, column=3)
e7.insert(0,"0.5")

e8 = Entry(root)
e8.grid(row=9, column=1)
e8.insert(0,"0")

e9 = Entry(root)
e9.grid(row=10,column=1)
e9.insert(0,"5")


def browse_button1():
    global directory
    filename1 = filedialog.askdirectory()
    directory = filename1

    Label(root, text=directory).grid(row=0, column=1)

button_clean = Button(root, text="Browse", command=browse_button1).grid(row=0, column=3)

def browse_button2():
    global save_path
    filename2 = filedialog.askdirectory()
    save_path = filename2

    Label(root, text = save_path).grid(row=1, column=1)

button_save = Button(root, text="Browse", command=browse_button2).grid(row=1, column=3)

#Create empty Dataframe
alerts = pd.DataFrame()

def scan():
    global alerts
    
    ma_window = int(e1.get())
    price_threshold = int(e2.get())
    volume_threshold = int(e3.get())
    range1 = float(e4.get())
    range2 = float(e5.get())
    range3 = float(e6.get())
    range4 = float(e7.get())
    
    #loop over all files in directory
    for entry in os.scandir(directory):
        if (entry.path.endswith(".csv")) and entry.is_file():
            
            data = pd.read_csv(entry.path)
        
            #Define exchange name and coin name from folder path
            filename = ntpath.basename(entry.path)
            fn = filename.split('_')
            exchange = fn[0]
            coin1 = fn[1]
            coin2 = fn[2]
            binance = fn[1] + "-" + fn[2]
        
            #Add exchange name and coin name to dataframe
            data.insert(1,column='Exchange', value=exchange)
            data.insert(2,column='Coin',value=coin1)
            data.insert(3,column="Binance",value=binance)
        
            #Convert Timestamp to a date format - is string 
            data['Timestamp']=pd.to_datetime(data['Timestamp'])
        
            #Add unix epoche and unix time indicator
            data['Unix_Time']=data['Timestamp'].view('int64')
            data['Unix_Indicator']=data['Unix_Time']/1000000000 % 3600 / 3600
        
            #Add moving average over closing price and calculate delta to closing price
            data['MA_Close']=data.Close.rolling(window=ma_window).mean()
            data['Delta_Price_%']= round((abs(data.MA_Close - data.Close))/data.MA_Close*100, 2)
        
            #Add moving average over vloume and calculate delta to volume
            data['MA_Volume']=data.Volume.rolling(window=ma_window).mean()
            data['Delta_Volume_%'] = round((abs(data.MA_Volume - data.Volume))/data.MA_Volume*100, 2)
        
            #Pump or Dump
            data['P&D1']= (data.MA_Close-data.Close)/data.MA_Close*100
            data.loc[data['P&D1'] >= price_threshold, 'P&D2'] = 'Dump'
            data.loc[data['P&D1'] <= (-1 * price_threshold), 'P&D2'] = 'Pump'
       
        
            #Identify subset of data - changepoint detection
            delta_data = data.loc[(data['Delta_Price_%'] >= price_threshold) & 
                              (data['Delta_Volume_%'] >= volume_threshold) & 
                              (data['Unix_Indicator'].between(range1,range2)) | 
                              
                              (data['Delta_Price_%'] >= price_threshold) & 
                              (data['Delta_Volume_%'] >= volume_threshold) & 
                              (data['Unix_Indicator'].between(range3,range4)) ]
        
            #Add the identified data to the overall dataframe
            alerts = alerts.append(delta_data)

    #Remove irrelevant columns
    alerts = alerts.drop(columns=['Unix_Time','MA_Close','MA_Volume','P&D1'])
    #Sort results based on Date
    alerts = alerts.sort_values(by='Timestamp')

    alerts = alerts.reset_index()
    alerts = alerts.drop(columns ="index")

    #define folder path incl. filename
    output_path = save_path + '\\' + exchange + '_'+ 'alerts' + '_' + 'window' + '-' + str(ma_window) + '_' + 'price' + '-' + str(price_threshold) + '_' + 'volume' + '-' + str(volume_threshold) + '_' + 'findings' + '_' + str(len(alerts)) + '.csv' 

    #create and save CSV
    alerts.to_csv(output_path, index = False)

    #Count Amount of Pumps and Dumps
    pumps = sum(1 for i in alerts['P&D2'] if i == 'Pump')
    dumps = sum(1 for i in alerts['P&D2'] if i == 'Dump')

    #Show findings
    print(exchange)
    print('Alerts = ' + str(len(alerts)))
    print('Pumps = ' + str(pumps))
    print('Dumps = ' + str(dumps))
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(alerts)
    
    return alerts
    return exchange
    return directory

button_scan = Button(root, text="Start Scan", command=scan, bg = '#00D700').grid(row=8, column=0)

def plot():
    
    idx = int(e8.get())
    candle = int(e9.get())

    #Select row
    plot = alerts.iloc[idx]

    #define the filename by taking the binance information
    fn1 = plot['Binance'].split('-')
    coin3 = fn1[0]
    coin4 = fn1[1] 
    exchange = plot['Exchange']
    plotfilename = exchange + '_' + coin3 + '_' + coin4 + '_clean.csv'

    #Open the file
    plotfile = pd.read_csv(directory + '\\' + plotfilename)

    #Convert timestamp to datetime format
    plotfile['Timestamp']=pd.to_datetime(plotfile['Timestamp'])

    #Define Range
    timestamp = plot['Timestamp']
    start_date = timestamp - timedelta(hours=2) 
    end_date = timestamp + timedelta(hours=2)

    mask = (plotfile['Timestamp'] > start_date) & (plotfile['Timestamp'] <= end_date)
    plotdata = plotfile.loc[mask]

    #Resample the data based on the desired candle size
    plotdata = plotdata.set_index('Timestamp')
    plotdata = plotdata.resample(str(candle)+'Min').agg({'Open': 'first', 
                                                        'High': 'max',
                                                        'Low': 'min', 
                                                        'Close': 'last'})

    plotdata.insert(0,column='Timestamp', value=pd.to_datetime(plotdata.index))

    #Define Candlestick
    fig = go.Figure(data=[go.Candlestick(x = plotdata['Timestamp'],
                                             open = plotdata['Open'],
                                             high = plotdata['High'],
                                             low = plotdata['Low'],
                                             close = plotdata['Close'])])
    fig.show()

button_visualize = Button(root, text = "Show Plot!", command=plot, bg = '#00D700').grid(row=11, column=0)
    
mainloop( )


# In[ ]:





# In[ ]:




