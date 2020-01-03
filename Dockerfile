FROM python:3

COPY ccxt_worker.py ./
COPY ccxt_scheduler.py ./

RUN pip install ccxt pandas schedule pickledb

CMD [ "python", "./ccxt_scheduler.py" ]