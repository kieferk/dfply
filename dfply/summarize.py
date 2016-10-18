from .base import *


@dfpipe
def summarize(df, **kwargs):
    return pd.DataFrame({k:[v] for k,v in kwargs.items()})


@dfpipe
def summarize_each(df, functions, *args):
    columns, values = [], []
    for arg in args:
        if isinstance(arg, pd.Series):
            varname = arg.name
            col = arg
        elif isinstance(arg, str):
            varname = arg
            col = df[varname]
        elif isinstance(arg, int):
            varname = df.columns[arg]
            col = df.iloc[:, arg]

        for f in functions:
            fname = f.__name__
            columns.append('_'.join([varname, fname]))
            values.append(f(col))

    return pd.DataFrame([values], columns=columns)


# ------------------------------------------------------------------------------
# Series summary functions
# ------------------------------------------------------------------------------

def _order_by(series, order_series):
    assert series.shape[0] == order_series.shape[0]
    if isinstance(order_series, pd.Series):
        sorted_series = pd.DataFrame({
            'series':series.values,
            'order':order_series.values
        }).sort_values('order', ascending=True)['series']
        return sorted_series
    elif isinstance(order_series, (list, tuple)):
        sorter = pd.concat(list(order_series), axis=1)
        sorter_columns = ['_sorter'+str(i) for i in range(sorter.shape[1])]
        sorter.columns = sorter_columns
        sorter['series'] = series.values
        sorted_series = sorter.sort_values(sorter_columns)['series']
        return sorted_series


def desc(series):
    descending = pd.DataFrame({
        'series':series.values,
        'order':np.arange(series.shape[0])
    }).sort_values('series', ascending=False)
    descending['desc'] = np.arange(series.shape[0])
    return descending.sort_values('order', ascending=True)['desc']


def mean(series):
    mean_s = series.mean()
    return mean_s


def first(series, order_by=None):
    if order_by is not None:
        series = _order_by(series, order_by)
    first_s = series.iloc[0]
    return first_s


def last(series, order_by=None):
    if order_by is not None:
        series = _order_by(series, order_by)
    last_s = series.iloc[series.size - 1]
    return last_s


def nth(series, n, order_by=None):
    if order_by is not None:
        series = _order_by(series, order_by)
    try:
        return series.iloc[n]
    except:
        return np.nan


def n(series):
    n_s = series.size
    return n_s


def n_distinct(series):
    n_distinct_s = series.unique().size
    return n_distinct_s


def IQR(series):
    iqr_s = series.quantile(.75) - series.quantile(.25)
    return iqr_s


def colmin(series):
    min_s = series.min()
    return min_s


def colmax(series):
    max_s = series.max()
    return max_s


def median(series):
    median_s = series.median()
    return median_s


def var(series):
    var_s = series.var()
    return var_s


def sd(series):
    sd_s = series.std()
    return sd_s
