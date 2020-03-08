#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
  
# Get the list of all files and directories 
# in the root directory 

#INSTRUCTION: Provide the directory path with the raw data
path = "C:\\Users\\florian.bozzone\\Desktop\\CCXT\\binance"
dir_list = os.listdir(path) 

#INSTRUCTION: Provide the directory path where you want to save the merged and clean files
path_save = "C:\\Users\\florian.bozzone\\Desktop\\CCXT\\binance_clean"


# In[2]:


def extract_data_and_save(idx, coin_1, coin_2):
    # Define target
    index = idx
    coin1 = coin_1
    coin2 = coin_2
    output_fn = index + '_' + coin1 + '_' + coin2 + '_sorted.csv'

    # Create string
    search_header = index + '_' + coin1 + '-' + coin2

    # Search loop
    list_with_filenames = []
    for foo in dir_list:
        # Check if we are interested in file
        if search_header in foo:
            list_with_filenames.append(foo)    
    #print(list_with_filenames)
    # Create empty dataframe
    data = pd.DataFrame()

    # Iterate over every entry in list
    for fn in list_with_filenames:
        # Create absolut path of file to read
        tmp_path = os.path.join(path,fn)
        # Read data
        tmp_data = pd.read_csv(tmp_path)
        # Append tmp_data to main dataframe
        data = data.append(tmp_data)

    # Sort dataframe by timestamp
    data.sort_values(by=['Timestamp'], inplace=True)
    # Remove first column
    data = data.iloc[:,1:]
    # dropping duplicte values, keep first occurence 
    data.drop_duplicates(subset ='Timestamp', keep = 'first', inplace = True)
    # Create absolut path of file to save
    tmp_fn = os.path.join(path_save,output_fn)
    # Store data in file
    data.to_csv(tmp_fn,index=False)


# In[3]:


# Create a dict of filename starts
coin_name_dict = dict()
for item in dir_list:
    if '[' in item:
        # Extract start of fn
        foo = item.split('[')[0]
        # Save it in dict
        coin_name_dict[foo] = 0

# Generate a list of keys -> different filename starts
coin_name_list = list(coin_name_dict)
#print(coin_name_list)

# Use keys to call extraction function
for fn in coin_name_list:
    tmp_fn = fn.split('_')
    tmp_index = tmp_fn[0]
    tmp_fn = tmp_fn[1].split('-')
    tmp_coin1 = tmp_fn[0]
    tmp_coin2 = tmp_fn[1]
    
    # Call function
    extract_data_and_save(tmp_index, tmp_coin1, tmp_coin2)

