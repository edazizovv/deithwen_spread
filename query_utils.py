#
import json
import datetime

#
import pandas
import urllib3


#


#
class MOEX:
    @staticmethod
    def get_forts_futures_names_by_filter(base_security):
        # https://iss.moex.com/iss/engines/futures/markets/forts/securities.xml

        engine = 'futures'
        market = 'forts'

        http = urllib3.PoolManager()

        url = 'https://iss.moex.com/iss/engines/{engine}/markets/{market}/securities.json' \
            .format(engine=engine, market=market)

        r = http.request('GET', url,
                         headers={'Content-Type': 'application/json'})

        result = r.data

        data = json.loads(result.decode())

        data_frame = pandas.DataFrame(data=data['securities']['data'], columns=data['securities']['columns'])
        data_frame = data_frame[data_frame['ASSETCODE'] == base_security].copy()

        return data_frame

    @staticmethod
    def get_forts_futures_candles_by_name_and_time_frame(security, frequency='1m', start=None, end=None):

        engine = 'futures'
        market = 'forts'

        if not start:
            start = str(datetime.date.today())
        if not end:
            end = str(datetime.date.today())

        frequency_codes = {'1m': "1",
                           '1h': "60"}

        http = urllib3.PoolManager()

        url = 'https://iss.moex.com/iss/engines/{engine}/markets/{market}/securities/{security}/candles.json' \
              '?from={start}&till={end}&interval={interval}'.format(
            engine=engine, market=market, security=security,
            start=start, end=end, interval=frequency_codes[frequency])

        r = http.request('GET', url,
                         headers={'Content-Type': 'application/json'})

        result = r.data

        data = json.loads(result.decode())

        data_frame = pandas.DataFrame(data=data['candles']['data'], columns=data['candles']['columns'])

        return data_frame

    @staticmethod
    def get_shares_securities_candles_by_name_and_time_frame(security, frequency='1m', start=None, end=None):

        engine = 'stock'
        market = 'shares'

        if not start:
            start = str(datetime.date.today())
        if not end:
            end = str(datetime.date.today())

        frequency_codes = {'1m': "1",
                           '1h': "60"}

        http = urllib3.PoolManager()

        url = 'https://iss.moex.com/iss/engines/{engine}/markets/{market}/securities/{security}/candles.json' \
              '?from={start}&till={end}&interval={interval}'.format(
            engine=engine, market=market, security=security,
            start=start, end=end, interval=frequency_codes[frequency])

        r = http.request('GET', url,
                         headers={'Content-Type': 'application/json'})

        result = r.data

        data = json.loads(result.decode())

        data_frame = pandas.DataFrame(data=data['candles']['data'], columns=data['candles']['columns'])

        return data_frame
