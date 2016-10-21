from .base import *
import warnings
import numpy as np


# ------------------------------------------------------------------------------
# `head` and `tail`
# ------------------------------------------------------------------------------

@dfpipe
def head(df, n=5):
    return df.head(n)


@dfpipe
def tail(df, n=5):
    return df.tail(n)


# ------------------------------------------------------------------------------
# Sampling
# ------------------------------------------------------------------------------

@dfpipe
def sample(df, *args, **kwargs):
    return df.sample(*args, **kwargs)


@pipe
@group_delegation
@symbolic_reference
def distinct(df, *args, **kwargs):
    return df.drop_duplicates(list(args), **kwargs)


@dfpipe
@join_index_arguments
def row_slice(df, indices):
    if indices.dtype == bool:
        return df.loc[indices, :]
    else:
        return df.iloc[indices, :]


# ------------------------------------------------------------------------------
# Filtering/masking
# ------------------------------------------------------------------------------

@dfpipe
def mask(df, *args):
    mask = pd.Series(np.ones(df.shape[0], dtype=bool))
    for arg in args:
        if arg.dtype != bool:
            raise Exception("Arguments must be boolean.")
        mask = mask & arg
    return df[mask.values]
