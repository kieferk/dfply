from .base import *


@dfpipe
def summarize(df, **kwargs):
    return pd.DataFrame({k:[v] for k,v in kwargs.items()})


@dfpipe
def summarize_each(df, functions, *args):
    columns, values = [], []
    for arg in args:
        varname = arg._name
        for f in functions:
            fname = f.__name__
            columns.append('_'.join([varname, fname]))
            values.append(f(arg))
    return pd.DataFrame([values], columns=columns)


# @dfpipe
# def count(df):
#     return pd.DataFrame(dict(n=[df.shape[0]]))
#
#
# @Pipe
# def nrow(df):
#     return df.shape[0]
