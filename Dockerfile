FROM python:3

COPY ccxt_worker.py ./
COPY ccxt_scheduler.py ./
COPY ccxt_hdfs_connector.py ./
COPY hosts ./

RUN cat hosts >> /etc/hosts
RUN cat /etc/hosts

RUN pip install ccxt pandas schedule pickledb hdfs

CMD [ "python", "./ccxt_scheduler.py" ]