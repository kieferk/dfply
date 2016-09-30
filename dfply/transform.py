from .base import *


@dfpipe
def mutate(df, **kwargs):
    df = df.copy()
    for key, value in kwargs.items():
        df[key] = value
    return df


@dfpipe
def transmute(df, **kwargs):
    df = df.copy()
    for key, value in kwargs.items():
        df[key] = value
    return df.select(lambda x: x in kwargs.keys(), axis=1)


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
