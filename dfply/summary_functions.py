from .base import *
from .vector import *


# ------------------------------------------------------------------------------
# Series summary functions
# ------------------------------------------------------------------------------


@make_symbolic
def mean(series):
    """
    Returns the mean of a series.

    Args:
        series (pandas.Series): column to summarize.
    """

    if np.issubdtype(series.dtype, np.number):
        return series.mean()
    else:
        return np.nan


@make_symbolic
def first(series, order_by=None):
    """
    Returns the first value of a series.

    Args:
        series (pandas.Series): column to summarize.

    Kwargs:
        order_by: a pandas.Series or list of series (can be symbolic) to order
            the input series by before summarization.
    """

    if order_by is not None:
        series = order_series_by(series, order_by)
    first_s = series.iloc[0]
    return first_s


@make_symbolic
def last(series, order_by=None):
    """
    Returns the last value of a series.

    Args:
        series (pandas.Series): column to summarize.

    Kwargs:
        order_by: a pandas.Series or list of series (can be symbolic) to order
            the input series by before summarization.
    """

    if order_by is not None:
        series = order_series_by(series, order_by)
    last_s = series.iloc[series.size - 1]
    return last_s


@make_symbolic
def nth(series, n, order_by=None):
    """
    Returns the nth value of a series.

    Args:
        series (pandas.Series): column to summarize.
        n (integer): position of desired value. Returns `NaN` if out of range.

    Kwargs:
        order_by: a pandas.Series or list of series (can be symbolic) to order
            the input series by before summarization.
    """

    if order_by is not None:
        series = order_series_by(series, order_by)
    try:
        return series.iloc[n]
    except:
        return np.nan


@make_symbolic
def n(series):
    """
    Returns the length of a series.

    Args:
        series (pandas.Series): column to summarize.
    """

    n_s = series.size
    return n_s


@make_symbolic
def n_distinct(series):
    """
    Returns the number of distinct values in a series.

    Args:
        series (pandas.Series): column to summarize.
    """

    n_distinct_s = series.unique().size
    return n_distinct_s


@make_symbolic
def IQR(series):
    """
    Returns the inter-quartile range (IQR) of a series.

    The IRQ is defined as the 75th quantile minus the 25th quantile values.

    Args:
        series (pandas.Series): column to summarize.
    """

    iqr_s = series.quantile(.75) - series.quantile(.25)
    return iqr_s


@make_symbolic
def colmin(series):
    """
    Returns the minimum value of a series.

    Args:
        series (pandas.Series): column to summarize.
    """

    min_s = series.min()
    return min_s


@make_symbolic
def colmax(series):
    """
    Returns the maximum value of a series.

    Args:
        series (pandas.Series): column to summarize.
    """

    max_s = series.max()
    return max_s


@make_symbolic
def median(series):
    """
    Returns the median value of a series.

    Args:
        series (pandas.Series): column to summarize.
    """

    if np.issubdtype(series.dtype, np.number):
        return series.median()
    else:
        return np.nan


@make_symbolic
def var(series):
    """
    Returns the variance of values in a series.

    Args:
        series (pandas.Series): column to summarize.
    """
    if np.issubdtype(series.dtype, np.number):
        return series.var()
    else:
        return np.nan


@make_symbolic
def sd(series):
    """
    Returns the standard deviation of values in a series.

    Args:
        series (pandas.Series): column to summarize.
    """

    if np.issubdtype(series.dtype, np.number):
        return series.std()
    else:
        return np.nan
