from .base import *
import warnings
import numpy as np


# ==============================================================================
#
# SUBSETTERS
#
# These functions are for subsetting along rows (axis=0).
#
# ==============================================================================


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


@Pipe
@GroupDelegation
@SymbolicReference
def distinct(df, *args, **kwargs):
    drop_indices = df.drop_duplicates(*args, **kwargs).index
    return df.loc[drop_indices]


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
    print mask.shape
    print mask
    return df[mask.values]
