FROM python:3

COPY ccxt_worker.py ./
COPY ccxt_scheduler.py ./
COPY ccxt_hdfs_connector.py ./
COPY hosts ./
COPY start.sh ./

RUN chmod a+x start.sh

RUN pip install ccxt pandas schedule pickledb hdfs

#CMD [ "python", "./ccxt_scheduler.py" ]
CMD ["start.sh"]