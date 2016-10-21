from .base import *

# ------------------------------------------------------------------------------
# Series summary functions
# ------------------------------------------------------------------------------

@make_symbolic
def mean(series):
    mean_s = series.mean()
    return mean_s


@make_symbolic
def first(series, order_by=None):
    if order_by is not None:
        series = order_series_by(series, order_by)
    first_s = series.iloc[0]
    return first_s


@make_symbolic
def last(series, order_by=None):
    if order_by is not None:
        series = order_series_by(series, order_by)
    last_s = series.iloc[series.size - 1]
    return last_s


@make_symbolic
def nth(series, n, order_by=None):
    if order_by is not None:
        series = order_series_by(series, order_by)
    try:
        return series.iloc[n]
    except:
        return np.nan


@make_symbolic
def n(series):
    n_s = series.size
    return n_s


@make_symbolic
def n_distinct(series):
    n_distinct_s = series.unique().size
    return n_distinct_s


@make_symbolic
def IQR(series):
    iqr_s = series.quantile(.75) - series.quantile(.25)
    return iqr_s


@make_symbolic
def colmin(series):
    min_s = series.min()
    return min_s


@make_symbolic
def colmax(series):
    max_s = series.max()
    return max_s


@make_symbolic
def median(series):
    median_s = series.median()
    return median_s


@make_symbolic
def var(series):
    var_s = series.var()
    return var_s


@make_symbolic
def sd(series):
    sd_s = series.std()
    return sd_s
