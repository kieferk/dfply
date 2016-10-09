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
# Series operation helper functions
# ------------------------------------------------------------------------------

def lead(series, i=1):
    shifted = series.shift(i)
    return shifted


def lag(series, i=1):
    shifted = series.shift(i * -1)
    return shifted


def row_number(series):
    row_numbers = np.arange(len(series))
    return row_numbers


def between(series, a, b, inclusive=True):
    if inclusive == True:
        met_condition = (series >= a) & (series <= b)
    elif inclusive == False:
        met_condition = (series > a) & (series < b)
    return met_condition


def dense_rank(series, ascending=True):
    ranks = series.rank(method='dense', ascending=ascending)
    return ranks


def min_rank(series, ascending=True):
    ranks = series.rank(method='min', ascending=ascending)
    return ranks


def cumsum(series):
    sums = series.cumsum()
    return sums


def cummean(series):
    means = series.expanding().mean()
    return means


def cummax(series):
    maxes = series.expanding().max()
    return maxes
