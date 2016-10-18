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

def mean(series):
    mean_s = series.mean()
    return mean_s


def first(series):
    first_s = series.iloc[0]
    return first_s


def last(series):
    last_s = series.iloc[series.size - 1]
    return last_s


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
