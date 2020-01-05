from hdfs import InsecureClient


def get_hadoop_cluster():
    client_hdfs = InsecureClient('http://localhost:9870', user='user')
    content = client_hdfs.content('user/hue/ccxt')
    print(content)


get_hadoop_cluster()