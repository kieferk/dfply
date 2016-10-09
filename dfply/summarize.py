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


# @dfpipe
# def count(df):
#     return pd.DataFrame(dict(n=[df.shape[0]]))
#
#
# @Pipe
# def nrow(df):
#     return df.shape[0]
