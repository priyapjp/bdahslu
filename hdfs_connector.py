
import pandas as pd 
from hdfs import InsecureClient
import os

# Connecting to Webhdfs by providing hdfs host ip and webhdfs port (50070 by default)
client_hdfs = InsecureClient('http://172.17.0.1:9870/', user='hue')

# content = client_hdfs.list('/user/hue/ccxt')
# status= client_hdfs.status('/user/hue/ccxt')
# print(content)
# print(status)

# Creating a simple Pandas DataFrame
liste_hello = ['hello1','hello2'] 
liste_world = ['world1','world2']
df = pd.DataFrame(data = {'hello' : liste_hello, 'world': liste_world})

# Writing Dataframe to hdfs
with client_hdfs.write('ccxt/helloworld.csv', encoding = 'utf-8') as writer:
	df.to_csv(writer)

