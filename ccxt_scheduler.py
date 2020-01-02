import schedule
import time
from datetime import datetime
import ccxt_worker as worker
import logging

logging.basicConfig(filename='ccxt_app.log', level=logging.INFO)


def get_timestamp_now():
    return datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")


def job():
    print("I'm Working ...." + get_timestamp_now())
    logging.info("I'm working..." + get_timestamp_now())
    pull()


def pull():
    logging.info("----- Start pulling Data: " + get_timestamp_now())
    from_date = '2019-12-31 00:00:00'
    exchanges = ['kraken']
    for e in exchanges:
        worker.pull_data(e, from_date, 1000, '1m', '/Users/lukas/development')
    logging.info("-----  Pulling Data completed: " + get_timestamp_now())


schedule.every(3).hours.do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)
