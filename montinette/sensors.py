from functools import reduce
import pandas as pd
from pandas.util._validators import validate_bool_kwarg
import numpy as np

from .constants import PROJECT_TIMEZONE, COUNTER_COL, DATETIME_COL, UUID_COL
from .utils import reset_count_on_interval



class CounterSeries(pd.Series):
    @property
    def _constructor(self):
        return CounterSeries

    @property
    def _constructor_expanddim(self):
        return CounterDataFrame

class CounterDataFrame(pd.DataFrame):

    def __init__(self, data, datetime=DATETIME_COL, counter=COUNTER_COL, name=UUID_COL,
                  timestamp=True, timezone='utc', **kwargs):

        original2default = {datetime: DATETIME_COL,
                            counter: COUNTER_COL,
                            name: UUID_COL}

        columns = None

        if isinstance(data, pd.DataFrame):
            cdf = data.rename(columns=original2default)
            columns = cdf.columns

        # Dictionary
        elif isinstance(data, dict):
            cdf = pd.DataFrame.from_dict(data).rename(columns=original2default)
            columns = cdf.columns

        # List
        elif isinstance(data, list) or isinstance(data, np.ndarray):
            cdf = data
            columns = []
            num_columns = len(data[0])
            for i in range(num_columns):
                try:
                    columns += [original2default[i]]
                except KeyError:
                    columns += [i]

        elif isinstance(data, pd.core.internals.BlockManager):
            cdf = data

        else:
            raise TypeError('DataFrame constructor called with incompatible data and dtype: {e}'.format(e=type(data)))

        kwargs.pop('columns', None)
        super(CounterDataFrame, self).__init__(cdf, columns=columns, **kwargs)

        if self._has_counter_columns():
            self._set_counter(timestamp=timestamp, timezone=timezone, inplace=True)


    def _detect_reset_count(self, inplace=False):
        inplace = validate_bool_kwarg(inplace, 'inplace')

        if not inplace:
            data = self.copy()
        else:
            data = self

        if 'reset' in data.columns:
            data.rename(columns={'reset':'user_reset_cp'}, inplace=True)

        data = data[~data[COUNTER_COL].isna()]
        data = data.sort_values(DATETIME_COL)
        data['reset'] = ((data[COUNTER_COL].diff() < 0) & (data[COUNTER_COL] <= 10)).cumsum()


        if data['reset'].value_counts().shape[0] > 1:
            print('Resets in counts have been detected. Column "reset" created.')
        else:
            print('No resets detected.')

        if not inplace:
            return data

    def _detect_count_reduction(self, inplace=False):
        inplace = validate_bool_kwarg(inplace, 'inplace')
        if not inplace:
            data = self.copy()
        else:
            data = self

        while data[data.groupby('reset')[COUNTER_COL].diff() < 0].shape[0] > 0:
            data = data[~(data.groupby('reset')[COUNTER_COL].diff() < 0)]

        if not inplace:
            return data

    def _clean(self, inplace=False):
        inplace = validate_bool_kwarg(inplace, 'inplace')

        if not inplace:
            data = self.copy()
            data = data._detect_reset_count()._detect_count_reduction()
            return data

        else:
            self._detect_reset_count(inplace=inplace)
            self._detect_count_reduction(inplace=inplace)

    def _delta(self, clean=True, inplace=False, reset_interval=0):
        inplace = validate_bool_kwarg(inplace, 'inplace')
        if not inplace:
            data = self.copy()
        else:
            data = self

        if clean:
            data = data._clean(inplace=inplace)

        if 'count_delta' in data.columns:
            data.rename(columns={'count_delta':'user_delta_cp'}, inplace=True)

        # day interval
        data = reset_count_on_interval(data, interval=reset_interval, time_col=DATETIME_COL)

        cpt_in_lst = []
        for _, data_r in data.groupby(['reset', 'reset_interval']):
            data_r = data_r.sort_values(DATETIME_COL)
            if reset_interval > 0:
                data_r['count_delta'] = ((data_r[COUNTER_COL] - data_r[COUNTER_COL].iloc[0]) -
                                         (data_r[COUNTER_COL] - data_r[COUNTER_COL].iloc[0]).shift().fillna(0))
                #print(data_r)
            else:
                data_r['count_delta'] = data_r[COUNTER_COL] - data_r[COUNTER_COL].shift().fillna(0)
            cpt_in_lst.append(data_r)
        data = pd.concat(cpt_in_lst)

        if not inplace:
            return data

    def get_counts(self, freq='15T', clean=True, reset_interval=0):
        """
        Paramaters
        ----------
        freq: str (Default: '15T')
            The offset string or object representing target conversion (see
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects).
        clean: bool (Default: True)
            Perform a cleaning of the data before analysing it.
        reset_interval: int (Default: 0)
            Interval in day from which to reset the counter.
        """
        if 'count_delta' not in self.columns:
            data = self._delta(clean=clean, inplace=False, reset_interval=reset_interval).copy()

        result = data.set_index(DATETIME_COL).resample(freq)['count_delta'].sum().to_frame('count_delta')

        return result


    def get_hour_counts(self, clean=True, reset_interval=0):
        """
        Paramaters
        ----------
        clean: bool (Default: True)
            Perform a cleaning of the data before analysing it.
        reset_interval: int (Default: 0)
            Interval in day from which to reset the counter.
        """

        result = self.get_counts('1H', clean=clean, reset_interval=reset_interval)
        result = result.reset_index()

        result['date'] = result[DATETIME_COL].dt.date
        result['date'] = pd.to_datetime(result['date'])
        result['hr'] = result[DATETIME_COL].dt.hour
        result = result.set_index(['date', 'hr'])

        result = result.drop(columns=DATETIME_COL)

        return result

    def _set_counter(self, timestamp=False, timezone='utc', inplace=False):

        if not inplace:
            data = self.copy()
        else:
            data = self

        if timestamp:
            data[DATETIME_COL] = pd.to_datetime(data[DATETIME_COL])

        if not pd.core.dtypes.common.is_datetime64_any_dtype(data[DATETIME_COL].dtype):
            data[DATETIME_COL] = pd.to_datetime(data[DATETIME_COL])

        if not data[DATETIME_COL].dt.tz :
            data[DATETIME_COL] = data[DATETIME_COL].dt.tz_localize(timezone)
        if data[DATETIME_COL].dt.tz.__str__().lower != PROJECT_TIMEZONE.lower():
            data[DATETIME_COL] = data[DATETIME_COL].dt.tz_convert(PROJECT_TIMEZONE)

        if not pd.core.dtypes.common.is_float_dtype(data[COUNTER_COL].dtype):
            data[COUNTER_COL] = data[COUNTER_COL].astype("float")

        if not pd.core.dtypes.common.is_string_dtype(data[UUID_COL].dtype):
            data[UUID_COL] = data[UUID_COL].astype(str)


        if not inplace:
            return data

    def _has_counter_columns(self):

        if (COUNTER_COL in self) and (DATETIME_COL in self) and (UUID_COL in self):
            return True

        return False

    def _is_counterframe(self):

        check_counter_col = (
            (COUNTER_COL in self) and
            (
                pd.core.dtypes.common.is_float_dtype(self[COUNTER_COL]) or
                pd.core.dtypes.common.is_integer_dtype(self[COUNTER_COL])
            )
        )

        check_datetime_col = (
            (DATETIME_COL in self) and
            pd.core.dtypes.common.is_datetime64_any_dtype(self[DATETIME_COL])
        )

        check_uuid_col = (
        (UUID_COL in self) and
                (pd.core.dtypes.common.is_string_dtype(self[UUID_COL]))
        )

        if check_counter_col and  check_datetime_col and check_uuid_col:
            return True

        return False

    def __getitem__(self, key):
        """
        If the result is a column containing only 'geometry', return a
        GeoSeries. If it's a DataFrame with any columns of GeometryDtype,
        return a GeoDataFrame.
        """
        result = super(CounterDataFrame, self).__getitem__(key)
        if (isinstance(result, CounterDataFrame)) and result._is_counterframe():
            result.__class__ = CounterDataFrame

        elif isinstance(result, CounterDataFrame) and not result._is_counterframe():
            result.__class__ = pd.DataFrame

        elif isinstance(result, pd.DataFrame) and result._is_counterframe():
            result.__class__ = CounterDataFrame

        return result

    @property
    def _constructor(self):
        return CounterDataFrame

    @property
    def _constructor_sliced(self):
        return CounterSeries

    @property
    def _constructor_expanddim(self):
        return CounterDataFrame

    def __finalize__(self, other, method=None, **kwargs):

        """propagate metadata from other to self"""
        # merge operation: using metadata of the left object
        if method == "merge":
            for name in self._metadata:
                object.__setattr__(self, name, getattr(other.left, name, None))

        # concat operation: using metadata of the first object
        elif method == "concat":
            for name in self._metadata:
                object.__setattr__(self, name, getattr(other.objs[0], name, None))
        else:
            for name in self._metadata:
                object.__setattr__(self, name, getattr(other, name, None))

        return self

def merge_list_df(list_df):

    df = reduce(lambda df1,df2: df1.join(df2, how='inner', rsuffix='_X'), list_df)
    df['count_delta'] = df.sum(axis=1)
    return df[['count_delta']]

def count_veh_in_spot(cpt_in:CounterDataFrame | list, cpt_out:CounterDataFrame, reset_interval:int = 0):
    if isinstance(cpt_in, list):
        cpt_in = merge_list_df(cpt_in)
    if isinstance(cpt_out, list):
        cpt_out = merge_list_df(cpt_out)

    cpt_in = cpt_in.copy()
    cpt_out = cpt_out.copy()
    
    cpt_in['in'] = cpt_in['count_delta']
    cpt_out['out'] = cpt_out['count_delta']

    # join in and out
    data = cpt_in.join(cpt_out, how='inner', lsuffix='_in')[['in', 'out']]

    # reset_interval
    data = data.reset_index()
    data = reset_count_on_interval(data, interval=reset_interval, time_col='timestamp')
    
    # compute delta and parked veh
    data['delta'] = data['in'] - data['out']
    data['nb_veh'] = data.groupby('reset_interval')['delta'].cumsum()

    return data