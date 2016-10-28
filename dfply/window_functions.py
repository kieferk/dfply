from .base import *

# ------------------------------------------------------------------------------
# Window functions
# ------------------------------------------------------------------------------

@make_symbolic
def lead(series, i=1):
    """
    Returns a series shifted forward by a value. `NaN` values will be filled
    in the beginning.

    Same as a call to `series.shift(i)`

    Args:
        series: column to shift forward.
        i (int): number of positions to shift forward.
    """

    shifted = series.shift(i)
    return shifted


@make_symbolic
def lag(series, i=1):
    """
    Returns a series shifted backwards by a value. `NaN` values will be filled
    in the end.

    Same as a call to `series.shift(-i)`

    Args:
        series: column to shift backward.
        i (int): number of positions to shift backward.
    """

    shifted = series.shift(i * -1)
    return shifted


@make_symbolic
def between(series, a, b, inclusive=False):
    """
    Returns a boolean series specifying whether rows of the input series
    are between values `a` and `b`.

    Args:
        series: column to compare, typically symbolic.
        a: value series must be greater than (or equal to if `inclusive=True`)
            for the output series to be `True` at that position.
        b: value series must be less than (or equal to if `inclusive=True`) for
            the output series to be `True` at that position.

    Kwargs:
        inclusive (bool): If `True`, comparison is done with `>=` and `<=`.
            If `False` (the default), comparison uses `>` and `<`.
    """

    if inclusive == True:
        met_condition = (series >= a) & (series <= b)
    elif inclusive == False:
        met_condition = (series > a) & (series < b)
    return met_condition


@make_symbolic
def dense_rank(series, ascending=True):
    """
    Equivalent to `series.rank(method='dense', ascending=ascending)`.

    Args:
        series: column to rank.

    Kwargs:
        ascending (bool): whether to rank in ascending order (default is `True`).
    """

    ranks = series.rank(method='dense', ascending=ascending)
    return ranks


@make_symbolic
def min_rank(series, ascending=True):
    """
    Equivalent to `series.rank(method='min', ascending=ascending)`.

    Args:
        series: column to rank.

    Kwargs:
        ascending (bool): whether to rank in ascending order (default is `True`).
    """

    ranks = series.rank(method='min', ascending=ascending)
    return ranks


@make_symbolic
def cumsum(series):
    """
    Calculates cumulative sum of values. Equivalent to `series.cumsum()`.

    Args:
        series: column to compute cumulative sum for.
    """

    sums = series.cumsum()
    return sums


@make_symbolic
def cummean(series):
    """
    Calculates cumulative mean of values. Equivalent to
    `series.expanding().mean()`.

    Args:
        series: column to compute cumulative mean for.
    """

    means = series.expanding().mean()
    return means


@make_symbolic
def cummax(series):
    """
    Calculates cumulative maximum of values. Equivalent to
    `series.expanding().max()`.

    Args:
        series: column to compute cumulative maximum for.
    """

    maxes = series.expanding().max()
    return maxes


@make_symbolic
def cummin(series):
    """
    Calculates cumulative minimum of values. Equivalent to
    `series.expanding().min()`.

    Args:
        series: column to compute cumulative minimum for.
    """

    mins = series.expanding().min()
    return mins


@make_symbolic
def cumprod(series):
    """
    Calculates cumulative product of values. Equivalent to
    `series.cumprod()`.

    Args:
        series: column to compute cumulative product for.
    """

    prods = series.cumprod()
    return prods


@make_symbolic
def cumany(series):
    """
    Calculates cumulative any of values. Equivalent to
    `series.expanding().apply(np.any).astype(bool)`.

    Args:
        series: column to compute cumulative any for.
    """

    anys = series.expanding().apply(np.any).astype(bool)
    return anys


@make_symbolic
def cumall(series):
    """
    Calculates cumulative all of values. Equivalent to
    `series.expanding().apply(np.all).astype(bool)`.

    Args:
        series: column to compute cumulative all for.
    """

    alls = series.expanding().apply(np.all).astype(bool)
    return alls


@make_symbolic
def percent_rank(series, ascending=True):
    if series.size == 1:
        return 0
    percents = (series.rank(method='min', ascending=ascending) - 1) / (series.size - 1)
    return percents
