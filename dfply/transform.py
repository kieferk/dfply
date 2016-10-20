from .base import *


@dfpipe
def mutate(df, **kwargs):
    for key, value in kwargs.items():
        df[key] = value
    return df


@dfpipe
def transmute(df, *keep_columns, **kwargs):
    for key, value in kwargs.items():
        df[key] = value
    columns = [k for k in kwargs.keys()]+list(keep_columns)
    return df.select(lambda x: x in columns, axis=1)


# ------------------------------------------------------------------------------
# Window functions
# ------------------------------------------------------------------------------

@make_symbolic
def lead(series, i=1):
    shifted = series.shift(i)
    return shifted


@make_symbolic
def lag(series, i=1):
    shifted = series.shift(i * -1)
    return shifted


# def row_number(series):
#     row_numbers = np.arange(len(series))
#     return row_numbers

@make_symbolic
def between(series, a, b, inclusive=False):
    if inclusive == True:
        met_condition = (series >= a) & (series <= b)
    elif inclusive == False:
        met_condition = (series > a) & (series < b)
    return met_condition


@make_symbolic
def dense_rank(series, ascending=True):
    ranks = series.rank(method='dense', ascending=ascending)
    return ranks


@make_symbolic
def min_rank(series, ascending=True):
    ranks = series.rank(method='min', ascending=ascending)
    return ranks


@make_symbolic
def cumsum(series):
    sums = series.cumsum()
    return sums


@make_symbolic
def cummean(series):
    means = series.expanding().mean()
    return means


def cummax(series):
    maxes = series.expanding().max()
    return maxes


def cummin(series):
    mins = series.expanding().min()
    return mins


def cumprod(series):
    prods = series.cumprod()
    return prods
