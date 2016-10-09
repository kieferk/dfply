from .base import *
import warnings
import pandas as pd

# ==============================================================================
#
# SET OPERATIONS
#
# These functions treat DataFrames like sets
#
# ==============================================================================


# ------------------------------------------------------------------------------
# `union`
# ------------------------------------------------------------------------------

def validate_set_ops(df, other):
    if df.columns.values.tolist() != other.columns.values.tolist():
        not_in_df = [col for col in other.columns if col not in df.columns]
        not_in_other = [col for col in df.columns if col not in other.columns]
        error_string = 'Error: not compatible.'
        if len(not_in_df):
            error_string += ' Cols in y but not x: ' + str(not_in_df) + '.'
        if len(not_in_other):
            error_string += ' Cols in x but not y: ' + str(not_in_other) + '.'
        raise ValueError(error_string)
    if len(df.index.names) != len(other.index.names):
        raise ValueError('Index dimension mismatch')
    if df.index.names != other.index.names:
        raise ValueError('Index mismatch')
    else:
        return


@pipe
def union(df, other, index=False, keep='first'):
    validate_set_ops(df, other)
    stacked = df.append(other)
    if index:
        stacked_reset_indexes = stacked.reset_index()
        index_cols = [col for col in stacked_reset_indexes.columns if col not in df.columns]
        index_name = df.index.names
        return_df = stacked_reset_indexes.drop_duplicates(keep=keep).set_index(index_cols)
        return_df.index.names = index_name
        return return_df
    else:
        return stacked.drop_duplicates(keep=keep)
