from .base import *

@dfpipe
def head(df, n=5):
    return df.head(n)


@dfpipe
def tail(df, n=5):
    return df.tail(n)
