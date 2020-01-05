import schedule
import time
from datetime import datetime
import ccxt_worker as worker
import logging
import pickledb

logging.basicConfig(filename='ccxt_app.log', level=logging.INFO)
db = pickledb.load('ccxt_config.db', True)


def write_start_date(start_date):
    db.set('start_date', start_date)
    db.dump()


def get_last_start_date():
    return db.get('start_date')


def get_timestamp_now():
    return datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")


def job():
    print("I'm Working ...." + get_timestamp_now())
    logging.info("I'm working..." + get_timestamp_now())
    pull()


def pull():
    start_time = get_timestamp_now()
    logging.info("----- Start pulling Data: " + start_time)
    from_date = get_last_start_date()

    if not from_date:
        from_date = get_timestamp_now()
        write_start_date(get_timestamp_now())

    print("Last Start: " + from_date)

    exchanges = ['kraken', 'binance', 'kucoin']
    for e in exchanges:
        try:
            worker.pull_data(e, from_date, 1000, '1m', '/Users/lukas/development')
            logging.info("-----  Pulling Data completed for : " + e + get_timestamp_now())
        except Exception:
            continue
    write_start_date(get_timestamp_now())


schedule.every(4).hours.do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)
