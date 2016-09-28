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

#
# def arg_indices_to_integer(f):
#     @wraps(f)
#     def arg_indices_to_integer_wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         rows = np.arange(args[0].shape[0])
#         positions = []
#         for ind in args[1:]:
#             if type(ind) in (tuple, list):
#                 ind = np.array(ind)
#             if type(ind) == pd.Series:
#                 ind = ind.values
#             if type(ind) == int:
#                 positions.append(np.atleast_1d(ind))
#             elif type(ind) == np.ndarray:
#                 if ind.dtype == int:
#                     positions.append(ind)
#                 elif ind.dtype == bool:
#                     positions.append(rows[ind])
#         if len(positions) == 0:
#             return f(args[0], list(positions), **kwargs)
#         elif len(positions) == 1:
#             return f(args[0], list(positions[0]), **kwargs)
#         else:
#             return f(args[0], list(reduce(np.intersect1d, positions)), **kwargs)
#     return arg_indices_to_integer_wrapped


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


@dfpipe
def distict(df, *args, **kwargs):
    drop_indices = df.drop_duplicates(**kwargs).index
    return df.loc[drop_indices]


@dfpipe
@join_index_arguments
def row_slice(df, indices):
    print indices
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
    return df[mask]
