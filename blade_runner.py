#


#
import pandas


#
from query_utils import MOEX
from examples import EF_SBRMX_00, CS_EXST0_00, ES_SBFX0_00


#
start, end = '2022-02-18', '2023-05-18'

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

# https://mfd.ru/marketdata/ticker/?id=29063#id=29063&start=21.09.2020&timeframe=1440&i0=EMA&i1=EMA&i1_Period=50&i2=MACD&count=NaN
futures_listed = ['SRH4', 'SRZ3', 'SRU3', 'SRM3', 'SRH3', 'SRZ2', 'SRU2', 'SRM2', 'SRH2', 'SRZ1', 'SRU1', 'SRM1',
                  'SRH1', 'SRZ0', 'SRU0', 'SRM0', 'SRH0']
element = EF_SBRMX_00(moex_loader=MOEX, saver=None,
                      commission_long_base=0, commission_short_base=0,
                      commission_long_futures=0, commission_short_futures=0,
                      frequency='1h', futures_listed=futures_listed,
                      start_hours=8, end_hours=18)
# element.source(start=start, end=end)
# element.ds(start=start, end=end, pos_one_long=True)
# element.dq(start=start, end=end, pos_one_long=True)

# TBD: should we fix datetime index to be continuous without gaps?

closer = CS_EXST0_00()
# resolved = closer.resolve(flow=element)
# """
es = ES_SBFX0_00(flow=element, closer=closer)
longs = []
shorts = []
for j in range(10):
    start_local = (pandas.to_datetime(start) + pandas.DateOffset(days=j)).date().__str__()
    end_local = (pandas.to_datetime(end) + pandas.DateOffset(days=j)).date().__str__()
    ln = es.long_result(start=start_local, end=end_local)
    st = es.short_result(start=start_local, end=end_local)
    longs.append(ln)
    shorts.append(st)
    es.flow.data_stock.to_excel('./data/data_stock_{0}.xlsx'.format(j))
    with pandas.ExcelWriter('./data/data_futures_{0}.xlsx'.format(j)) as writer:
        for security in es.flow.data_futures.keys():
            es.flow.data_futures[security].to_excel(writer, sheet_name=security)
    es.flow.ds_stock.to_excel('./data/ds_stock_{0}.xlsx'.format(j))
    es.flow.ds_futures.to_excel('./data/ds_futures_{0}.xlsx'.format(j))
    es.flow.dq_stock.to_excel('./data/dq_stock_{0}.xlsx'.format(j))
    es.flow.dq_futures.to_excel('./data/dq_futures_{0}.xlsx'.format(j))
    pandas.DataFrame(es.flow.base).to_excel('./data/base_{0}.xlsx'.format(j))
    pandas.DataFrame(es.flow.proxy).to_excel('./data/proxy_{0}.xlsx'.format(j))
longs = pandas.DataFrame(data=longs, columns=['resolution', 'days', 'yields'])
shorts = pandas.DataFrame(data=shorts, columns=['resolution', 'days', 'yields'])
longs.to_excel('./data/longs.xlsx')
shorts.to_excel('./data/shorts.xlsx')
# """