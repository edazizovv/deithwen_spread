#


#
import numpy
import pandas


#
# TODO: currently this approach is synchronous; consider advanced asynchronous approach

class FractionalFrame:
    def __init__(self, data_raw, date_name, entities, n_name='_n', solder_name='_solder',
                 solder_flows_name='_solder_flows', residual_flows_name='_residual_flows',
                 capital_flows_name='_capital_flows', capital_yields_name='_capital_yields',
                 capital_name='_capital', residual_name='_residual',
                 morning_hour_start=0, evening_hour_end=24,
                 entity_switch_end_slippage=None,
                 working_entity_len_threshold=10,
                 reserved_share=0.10,
                 buy_cm=0, sell_cm=0, funding_rates=None
                 ):

        self.data_raw = data_raw
        self.date_name = date_name
        self.n_name = n_name
        self.entities = entities
        self.solder_name = solder_name
        self.solder_flows_name = solder_flows_name
        self.residual_flows_name = residual_flows_name
        self.capital_flows_name = capital_flows_name
        self.capital_yields_name = capital_yields_name
        self.capital_name = capital_name
        self.residual_name = residual_name

        self.morning_hour_start = morning_hour_start
        self.evening_hour_end = evening_hour_end
        self.entity_switch_end_slippage = entity_switch_end_slippage
        self.working_entity_len_threshold = working_entity_len_threshold

        self.reserved_share = reserved_share
        self.buy_cm = buy_cm
        self.sell_cm = sell_cm
        if funding_rates:
            assert funding_rates.index == self.data_raw.index
            assert self.entities in funding_rates.columns
            self.funding_rates = funding_rates
        else:
            self.funding_rates = pandas.DataFrame(data=numpy.zeros(shape=(self.data_raw.shape[0], len(self.entities))),
                                                  columns=self.entities,
                                                  index=self.data_raw.index)

        self.data_raw[self.date_name] = pandas.to_datetime(self.data_raw[self.date_name])
        self.data_raw[self.n_name] = range(self.data_raw[0])
        self.index = self.data_raw.index

        self.entities_valid_indices = {}
        self.entities_working_indices = {}
        start_end_dates = []
        for entity in self.entities:
            entity_valid_mask = ~pandas.isna(self.data_raw[entity])
            entity_valid_index = self.data_raw[entity_valid_mask].index
            entity_working_mask = (self.data_raw[entity_valid_index].index.hour >= self.morning_hour_start) * \
                                  (self.data_raw[entity_valid_index].index.hour <= self.evening_hour_end)
            entity_working_index = self.data_raw[entity_working_mask]
            if self.entity_switch_end_slippage:
                entity_working_index = entity_working_index.iloc[:-self.entity_switch_end_slippage]
            entity_working_index = entity_working_index.index
            self.entities_valid_indices[entity] = entity_valid_index
            self.entities_working_indices[entity] = entity_working_index
            if entity_working_index.shape[0] >= self.working_entity_len_threshold:
                start_end_dates.append([entity, entity_working_index.min(), entity_working_index.max()])
        self.start_end_dates = pandas.DataFrame(data=start_end_dates, columns=['entity', 'start', 'end'])
        self.start_end_dates = self.start_end_dates.sort_values(by=['start', 'end'], ascending=True)

        previous_end = None
        drop_dates = []
        for j in range(self.start_end_dates.shape[0]):
            entity = self.start_end_dates['entity'].values[j]
            current_start, current_end = self.start_end_dates['start'].values[j], self.start_end_dates['end'].values[j]
            if j > 0:
                if current_end < previous_end:
                    drop_dates.append(self.start_end_dates.index[j])
                    continue
                elif current_start < previous_end:
                    update_working_mask = self.data_raw[self.entities_working_indices[entity]].index >= previous_end
                    update_working_index = self.data_raw[update_working_mask].index
                    self.entities_working_indices[entity] = update_working_index
                    self.start_end_dates.loc[self.start_end_dates.index[j], 'start'] = update_working_index.min()
            previous_end = self.start_end_dates['end'].values[j]
        self.start_end_dates = self.start_end_dates.drop(index=drop_dates)

        self.data_solder = self.data_raw.copy()
        last_entity_value = None
        self.joint_index = None
        for j in range(self.start_end_dates.shape[0]):
            entity = self.start_end_dates['entity'].values[j]
            working_index = self.entities_working_indices[entity]
            first_entity_value = self.data_solder.loc[working_index[0], entity]
            if j == 0:
                self.data_solder.loc[working_index, entity] = self.data_solder.loc[working_index, entity] / \
                                                              first_entity_value
                self.joint_index = working_index.copy()
            elif j > 0:
                self.data_solder.loc[working_index, entity] = self.data_solder.loc[working_index, entity] * \
                    (last_entity_value / first_entity_value)
                self.joint_index = self.joint_index.union(working_index)
            last_entity_value = self.data_solder.loc[working_index[-1], entity]
            self.data_solder.loc[working_index, self.solder_name] = self.data_solder.loc[working_index, entity].values
            self.data_solder.loc[self.residual_flows_name, self.residual_flows_name] = \
                -(self.data_raw.loc[working_index, entity] * self.funding_rates.loc[working_index, entity])
            self.data_solder.loc[working_index[1], self.residual_flows_name] -= \
                self.data_solder.loc[working_index[0], entity] * self.buy_cm
            self.data_solder.loc[working_index[-1], self.residual_flows_name] -= \
                self.data_solder.loc[working_index[-1], entity] * self.buy_cm

        self.data_solder.loc[self.joint_index, self.solder_flows_name] = \
            self.data_solder[self.joint_index, self.solder_name].diff()

        self.data_solder.loc[self.joint_index[0], self.residual_name] = self.reserved_share
        self.data_solder.loc[self.joint_index[1:], self.residual_name] = \
            self.data_solder.loc[self.joint_index[1:], self.residual_flows_name]
        self.data_solder.loc[self.joint_index, self.residual_name] = \
            self.data_solder.loc[self.joint_index, self.residual_name].cumsum()

        self.data_solder.loc[self.joint_index, self.capital_flows_name] = \
            self.data_solder.loc[self.joint_index, self.solder_flows_name] + \
            self.data_solder.loc[self.joint_index, self.residual_flows_name]

        self.data_solder.loc[self.joint_index[0], self.capital_name] = 1 + self.reserved_share
        self.data_solder.loc[self.joint_index[1:], self.capital_name] = \
            self.data_solder.loc[self.joint_index[1:], self.capital_flows_name]
        self.data_solder.loc[self.joint_index, self.capital_name] = \
            self.data_solder.loc[self.joint_index, self.capital_name].cumsum()

        self.data_solder.loc[self.joint_index, self.capital_yields_name] = \
            self.data_solder.loc[self.joint_index, self.capital_name].pct_change()


class SolidFrame:
    ...
