#


#
import numpy
import pandas
from pandas.tseries.offsets import DateOffset


#
from proto import ElementFlow


#
class EF_SBRMX_00(ElementFlow):
    def __init__(self, moex_loader, saver, commission_long_base, commission_short_base,
                 commission_long_futures, commission_short_futures, frequency='1m'):
        """
        Implements MOEX SBER stock
        """
        self.moex_loader = moex_loader
        self.saver = saver

        self.commission_long_base = commission_long_base
        self.commission_short_base = commission_short_base
        self.commission_long_futures = commission_long_futures
        self.commission_short_futures = commission_short_futures

        self.frequency = frequency

        self.futures_listed = None
        self.data_futures = None
        self.futures_base_security = 'SBRF'  # SBPR SBRF
        self.base_stock = 'SBER'
        self.data_stock = None

        self.ds_stock = None
        self.ds_futures = None

        self.ds_stock_base = None
        self.ds_futures_bases = None
        self.start_end_dates = None
        self.futures_ordered = None

        self.dq_stock = None
        self.dq_futures = None

        self.joint_index = None

        super().__init__()

    def source(self, start, end, update=True):

        futures_data = self.moex_loader.get_forts_futures_names_by_filter(base_security=self.futures_base_security)
        self.futures_listed = futures_data['SECID'].values

        self.data_futures = {}
        for security in self.futures_listed:
            sliced = self.moex_loader.get_forts_futures_candles_by_name_and_time_frame(security=security, frequency=self.frequency, start=start, end=end)
            sliced['end'] = pandas.to_datetime(sliced['end'])
            sliced = sliced.set_index('end')
            self.data_futures[security] = sliced

        self.data_stock = self.moex_loader.get_shares_securities_candles_by_name_and_time_frame(security=self.base_stock, frequency=self.frequency, start=start, end=end)

        self.data_stock = self.data_stock.set_index('begin', drop=False)
        self.data_stock.index = pandas.to_datetime(self.data_stock.index)

        for security in self.futures_listed:
            self.data_futures[security] = self.data_futures[security].set_index('begin', drop=False)
            self.data_futures[security].index = pandas.to_datetime(self.data_futures[security].index)

        # self.saver.save(self.data)  # TODO: implement saving & updating options

    def ds(self, start, end, pos_one_long, conj_thresh=3):

        self.source(start=start, end=end)

        data_stock = self.data_stock.copy()
        data_futures = dict(self.data_futures)

        joint_frame = data_stock[['close']].copy()
        for security in data_futures.keys():
            joint_frame = joint_frame.merge(right=data_futures[security][['close']].copy(), left_index=True, right_index=True, suffixes=('', security), how='outer')
        joint_index = joint_frame.index.copy()
        self.joint_index = joint_index.copy()

        data_stock = pandas.DataFrame(data=data_stock, index=joint_index)
        for security in self.futures_listed:
            data_futures[security] = pandas.DataFrame(data=data_futures[security], index=joint_index)

        cms_stock = self.commission_long_base if pos_one_long else self.commission_short_base
        cms_futures = self.commission_short_futures if pos_one_long else self.commission_long_futures

        mask_stock = (data_stock.index >= start) * (data_stock.index <= end)
        stock_capitalized = data_stock.loc[mask_stock, 'close'].diff().values[1:]
        stock_dividend = 0 # !!!
        stock_sum = stock_capitalized + stock_dividend
        ds_stock = pandas.DataFrame(data=numpy.concatenate(([stock_sum[0] + data_stock.loc[mask_stock, 'close'].values[0] * cms_stock],
                                      stock_sum[1:-1],
                                      [stock_sum[-1] + data_stock.loc[mask_stock, 'close'].values[-1] * cms_stock]),
                                     axis=0).reshape(-1, 1),
                                    columns=[self.base_stock],
                                    index=data_stock[mask_stock].index[1:].copy())

        start_end_dates = []
        for security in self.futures_listed:
            local_mask = (data_futures[security].index >= start) * (data_futures[security].index <= end)
            local_start, local_end = data_futures[security][local_mask].index.min(), data_futures[security][local_mask].index.max()
            start_end_dates.append([security, local_start, local_end])
        start_end_dates = pandas.DataFrame(data=start_end_dates, columns=['security', 'start', 'end'])
        start_end_dates = start_end_dates.sort_values(by=['start', 'end'], ascending=True)

        futures_ordered = []
        ds_futures = pandas.DataFrame(index=joint_index)
        take = None
        for j in range(start_end_dates.shape[0]):
            if j == 0:
                local_start, local_end = start_end_dates['start'].values[j], start_end_dates['end'].values[j]
                security = start_end_dates['security'].values[j]
                futures_mask = (data_futures[security].index >= local_start) * (
                        data_futures[security].index <= local_end)
            else:
                local_start, local_end = start_end_dates['start'].values[j], start_end_dates['end'].values[j]
                security = start_end_dates['security'].values[j]
                previous_end = take.index.max()
                if local_start < previous_end:
                    local_start = previous_end
                if local_end < previous_end:
                    continue                            # TBD! check and decide on this case
                futures_mask = (data_futures[security].index > local_start) * (
                        data_futures[security].index <= local_end)

            take = data_futures[security][futures_mask].copy()
            if j < start_end_dates.shape[0] - 1:
                take = take.iloc[:-conj_thresh, :].copy()
            if take.dropna().shape[0] == 0:
                continue
            # local_mask = (ds_futures.index > take.index.min()) * (ds_futures.index <= take.index.max())
            # ds_futures.loc[local_mask, security] = numpy.concatenate(([take['close'].diff()[1] -
            ds_futures.loc[take.index[1:], security] = numpy.concatenate(([take['close'].diff()[1] -
                                                                       take['close'].values[0] * cms_futures],
                                                                    take['close'].diff()[2:-1],
                                                                    [take['close'].diff()[-1] -
                                                                     take['close'].values[-1] * cms_futures]),
                                                                   axis=0)

            start_end_dates.loc[start_end_dates.index[j], 'end'] = take.index.max()
            futures_ordered.append(j)

            # self.ds_futures_bases[security] = take['close'].values[0] / (1 - cms_futures)     # !!!
        self.start_end_dates = start_end_dates
        self.futures_ordered = futures_ordered

        self.ds_stock = ds_stock
        self.ds_futures = ds_futures

    def dq(self, start, end, pos_one_long, conj_thresh=3):

        self.ds(start=start, end=end, pos_one_long=pos_one_long, conj_thresh=conj_thresh)

        data_stock = self.data_stock.copy()
        data_futures = dict(self.data_futures)

        data_stock = pandas.DataFrame(data=data_stock, index=self.joint_index)
        for security in self.futures_listed:
            data_futures[security] = pandas.DataFrame(data=data_futures[security], index=self.joint_index)

        mask_stock = (data_stock.index >= start) * (data_stock.index <= end)
        mask_ds_stock = (self.ds_stock.index >= start) * (self.ds_stock.index <= end)
        base_stock = data_stock.loc[mask_stock, ['close']].values
        dq_stock = self.ds_stock.loc[mask_ds_stock, :] / base_stock[:-1]
        # dq_stock = pandas.DataFrame(data=base_stock, index=self.data_stock.loc[mask_stock, 'close'].index.values[:-1])

        dq_futures = pandas.DataFrame(index=self.ds_futures.index[1:].copy())
        take = None
        for j in self.futures_ordered:
            if j == 0:
                local_start, local_end = self.start_end_dates['start'].values[j], self.start_end_dates['end'].values[j]
                security = self.start_end_dates['security'].values[j]
                futures_mask = (data_futures[security].index >= local_start) * (
                        data_futures[security].index <= local_end)

            else:
                local_start, local_end = self.start_end_dates['start'].values[j], self.start_end_dates['end'].values[j]
                security = self.start_end_dates['security'].values[j]
                futures_mask = (data_futures[security].index > local_start) * (
                        data_futures[security].index <= local_end)

            take = data_futures[security][futures_mask]
            # take = take.iloc[:-conj_thresh, :]
            # local_mask = (dq_futures.index > take.index.min()) * (dq_futures.index.index <= take.index.max())
            # dq_futures.loc[local_mask, 'yields'] = self.ds_futures.loc[local_mask, security] / self.data_futures[security][futures_mask].values[:-1]
            dq_futures.loc[take.index[1:], 'yields'] = self.ds_futures.loc[take.index[1:], security] / \
                                                   data_futures[security].loc[futures_mask, 'close'].iloc[:-1].values

        self.dq_stock = dq_stock
        self.dq_futures = dq_futures

    def dc(self, start, end, pos_one, conj_thresh=3):
        raise NotImplemented()


class EF_SBRFQ_00(ElementFlow):
    def __init__(self, moex_loader, saver, commission_long, commission_short):
        """
        Implements MOEX SBER quarterly futures
        """
        self.moex_loader = moex_loader
        self.saver = saver

        self.commission_long = commission_long
        self.commission_short = commission_short

        self.data = None

        super().__init__()

    def source(self, start, end, update=True):
        kwargs = {}
        self.data = self.moex_loader(start=start, end=end, **kwargs)
        # self.saver.save(self.data)  # TODO: implement saving & updating options

    def ds(self):
        pass
