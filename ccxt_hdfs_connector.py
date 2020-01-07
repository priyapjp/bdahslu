from hdfs import InsecureClient

client_hdfs = InsecureClient('http://172.17.0.1:9870', user='user')


def get_hadoop_cluster():
    content = client_hdfs.list('/user/hue/ccxt')
    print(content)


def write_to_hadoop_cluster(exchange_name, localpath):
    path_on_hdfs = '/user/hue/ccxt' + exchange_name
    client_hdfs.upload(path_on_hdfs, localpath)


get_hadoop_cluster()
