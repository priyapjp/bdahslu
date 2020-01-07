FROM python:3

COPY ccxt_worker.py ./
COPY ccxt_scheduler.py ./
COPY ccxt_hdfs_connector.py ./

RUN pip install ccxt pandas schedule pickledb hdfs

CMD [ "python", "./ccxt_scheduler.py" ]