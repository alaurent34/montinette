import pandas as pd
from pandas.util._validators import validate_bool_kwarg
import numpy as np


class CounterSeries(pd.Series):
    @property
    def _constructor(self):
        return CounterSeries

    @property
    def _constructor_expanddim(self):
        return CounterDataFrame

class CounterDataFrame(pd.DataFrame):

    def __init__(self, data, datetime="timestamp", counter='count', name='id',
                  timestamp=True):

        original2default = {datetime: "timestamp",
                            counter: 'count',
                            name: 'id'}

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

        super().__init__(cdf, columns=columns)

        if self._has_counter_columns():
            self._set_counter(timestamp=timestamp, inplace=True)


    def _detect_reset_count(self, inplace=False):
        inplace = validate_bool_kwarg(inplace, 'inplace')
        
        if not inplace:
            data = self.copy()
        else:
            data = self

        if 'reset' in data.columns:
            data.rename(columns={'reset':'user_reset_cp'}, inplace=True)

        data = data[~data['count'].isna()]
        data = data.sort_values('timestamp')
        data['reset'] = ((data['count'].diff() < 0) & (data['count'] <= 10)).cumsum()

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

        while data[data.groupby('reset')['count'].diff() < 0].shape[0] > 0:
            data = data[~(data.groupby('reset')['count'].diff() < 0)]

        if not inplace:
            return data
    
    def _clean(self, inplace=False):
        inplace = validate_bool_kwarg(inplace, 'inplace')
        if not inplace:
            data = self.copy()
        else:
            data = self

        #data = data.dropna()

        if not inplace:
            data = data._detect_reset_count()._detect_count_reduction()
            return data
        
        data._detect_reset_count(inplace=inplace)
        data._detect_count_reduction(inplace=inplace)

    def _delta(self, clean=True, inplace=False):
        inplace = validate_bool_kwarg(inplace, 'inplace')
        if not inplace:
            data = self.copy()
        else:
            data = self

        if clean:
            data = data._clean()

        if 'delta' in data.columns:
            data.rename(columns={'delta':'user_delta_cp'}, inplace=True)

        cpt_in_lst = []
        for _, data_r in data.groupby('reset'):
            data_r.sort_values('timestamp', inplace=True)
            data_r['delta'] = data_r['count'] - data_r['count'].shift().fillna(0)
            cpt_in_lst.append(data_r)
        data = pd.concat(cpt_in_lst)

        if not inplace:
            return data

    def get_hour_counts(self, clean=True):
        data = self.copy()

        if 'delta' not in data.columns:
            data = data._delta(clean=clean)

        result = data.groupby([data['timestamp'].dt.date, data['timestamp'].dt.hour])['delta'].sum().to_frame('delta')
        result.index.names = ['date', 'hr']

        return result 

    def _set_counter(self, timestamp=False, inplace=False):

        if not inplace:
            data = self.copy()
        else:
            data = self

        if timestamp:
            data['timestamp'] = pd.to_datetime(data['timestamp'])

        if not pd.core.dtypes.common.is_datetime64_any_dtype(data['timestamp'].dtype):
            data['timestamp'] = pd.to_datetime(data['timestamp'])

        if not pd.core.dtypes.common.is_float_dtype(data['count'].dtype):
            data['count'] = data['count'].astype("float")

        if not pd.core.dtypes.common.is_string_dtype(data['id'].dtype):
            data['id'] = data['id'].astype(str)

        if not inplace:
            return data

    def _has_counter_columns(self):

        if ('count' in self) and ('timestamp' in self) and ('id' in self):
            return True

        return False

    def _is_counterframe(self):

        if (('count' in self) and
                (pd.core.dtypes.common.is_float_dtype(self['count']) or
                 pd.core.dtypes.common.is_integer_dtype(self['count']))) \
            and (('timestamp' in self) and
                 pd.core.dtypes.common.is_datetime64_any_dtype(self['timestamp'])) \
            and (('id' in self) and
                (pd.core.dtypes.common.is_string_dtype(self['id']))):
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
