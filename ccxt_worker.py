import os
import time
import ccxt
import pandas as pd
from hdfs import InsecureClient


# plotting

def create_ohlcv_df(data):
    header = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(data, columns=header)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms', origin='unix')  # convert timestamp to datetime
    return df


def create_alternative_df():
    liste_hello = ['hello1', 'hello2']
    liste_world = ['world1', 'world2']
    df = pd.DataFrame(data={'hello': liste_hello, 'world': liste_world})
    return df


def pull_data(exchange, from_date, n_candles, c_size, f_path, skip=False):
    count = 1
    msec = 1000
    hold = 5  # waiting time between retry

    missing_symbols = []

    # -- create a folder --
    newpath = f_path + '/' + exchange + '/'
    # if not os.path.exists(newpath):
    #    os.makedirs(newpath)

    # -- load exchange --
    exc_instance = getattr(ccxt, exchange)()
    exc_instance.load_markets()
    from_timestamp = exc_instance.parse8601(from_date)

    # -- pull ohlcv --
    for symbol in exc_instance.symbols:
        for attempt in range(1):  # 1 attempts max
            try:
                print('Pulling:', exchange, ':', symbol, '[{}/{}]'.format(count, len(exc_instance.symbols)))
                data = exc_instance.fetch_ohlcv(symbol, c_size, from_timestamp, n_candles)

                # if < n_candles returned, skip this pair
                if len(data) < n_candles and skip is True:
                    continue

                # -- create DF --
                #df = create_ohlcv_df(data)
                df = create_alternative_df()

                # Skript changed by Fabio - START
                print(len(data))
                # Skript changed by Fabio - END

                # -- save CSV --
                symbol = symbol.replace("/", "-")
                filename = newpath + '{}_{}_[{}]-TO-[{}].csv'.format(exchange, symbol, df['Timestamp'].iloc[0],
                                                                     df['Timestamp'].iloc[-1])
                filenameonly = '{}_{}_[{}]-TO-[{}].csv'.format(exchange, symbol, df['Timestamp'].iloc[0],
                                                               df['Timestamp'].iloc[-1])
                # df.to_csv(filename)

                # -- save to HDFS --
                client_hdfs = InsecureClient('http://172.17.0.1:9870', user='hue')
                with client_hdfs.write('ccxtAutomated/' + exchange + '/' + filenameonly, encoding='utf-8') as writer:
                    df.to_csv(writer)

            except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.NetworkError,
                    ccxt.RequestTimeout,
                    IndexError) as error:
                print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
                time.sleep(hold)
            else:  # if no error, proceed to next iteration
                break
        else:  # we failed all attempts
            print('All attempts failed, skipping:', symbol)
            missing_symbols.append(symbol)
            continue

        count += 1
        # -- wait for rate limit --
        time.sleep((exc_instance.rateLimit / msec) + 3)  # rate limit +5 seconds to just to be safe

    # print out any symbols we could not obtain
    if len(missing_symbols) != 0:
        print('Unable to obtain:', missing_symbols)

    return missing_symbols
