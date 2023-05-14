#


#
import pandas


#
from query_utils import MOEX
from examples import EF_SBRMX_00


#
start, end = '2023-02-01', '2023-05-01'

"""
futures_data = MOEX.get_forts_futures_names_by_filter(base_security='SBPR')   # SBPR SBRF
futures_listed = futures_data['SECID'].values

data_futures = {}
for security in futures_listed:
    sliced = MOEX.get_forts_futures_candles_by_name_and_time_frame(security=security, start=start, end=end)
    sliced['end'] = pandas.to_datetime(sliced['end'])
    sliced = sliced.set_index('end')
    data_futures[security] = sliced

x1 = data_futures['SPM3']
x2 = data_futures['SPU3']

security = 'SBER'
data_stock = MOEX.get_shares_securities_candles_by_name_and_time_frame(security=security, start=start, end=end)
"""

element = EF_SBRMX_00(moex_loader=MOEX, saver=None,
                      commission_long_base=0, commission_short_base=0,
                      commission_long_futures=0, commission_short_futures=0,
                      frequency='1h')
# element.source(start=start, end=end)
# element.ds(start=start, end=end, pos_one_long=True)
element.dq(start=start, end=end, pos_one_long=True)
