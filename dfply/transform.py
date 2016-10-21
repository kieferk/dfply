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
