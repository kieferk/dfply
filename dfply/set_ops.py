from .base import *
import warnings
import pandas as pd


def validate_set_ops(df, other):
    """
    Helper function to ensure that DataFrames are valid for set operations.
    Columns must be the same name in the same order, and indices must be of the
    same dimension with the same names.
    """

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


# ------------------------------------------------------------------------------
# `union`
# ------------------------------------------------------------------------------

@pipe
def union(df, other, index=False, keep='first'):
    """
    Returns rows that appear in either DataFrame.

    Args:
        df (pandas.DataFrame): data passed in through the pipe.
        other (pandas.DataFrame): other DataFrame to use for set operation with
            the first.

    Kwargs:
        index (bool): Boolean indicating whether to consider the pandas index
            as part of the set operation (default `False`).
        keep (str): Indicates which duplicate should be kept. Options are `'first'`
            and `'last'`.
    """
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


# ------------------------------------------------------------------------------
# `intersect`
# ------------------------------------------------------------------------------


@pipe
def intersect(df, other, index=False, keep='first'):
    """
    Returns rows that appear in both DataFrames.

    Args:
        df (pandas.DataFrame): data passed in through the pipe.
        other (pandas.DataFrame): other DataFrame to use for set operation with
            the first.

    Kwargs:
        index (bool): Boolean indicating whether to consider the pandas index
            as part of the set operation (default `False`).
        keep (str): Indicates which duplicate should be kept. Options are `'first'`
            and `'last'`.
    """

    validate_set_ops(df, other)
    if index:
        df_reset_index = df.reset_index()
        other_reset_index = other.reset_index()
        index_cols = [col for col in df_reset_index.columns if col not in df.columns]
        df_index_names = df.index.names
        return_df = (pd.merge(df_reset_index, other_reset_index,
                              how='inner',
                              left_on=df_reset_index.columns.values.tolist(),
                              right_on=df_reset_index.columns.values.tolist())
                     .set_index(index_cols))
        return_df.index.names = df_index_names
        return_df = return_df.drop_duplicates(keep=keep)
        return return_df
    else:
        return_df = pd.merge(df, other,
                             how='inner',
                             left_on=df.columns.values.tolist(),
                             right_on=df.columns.values.tolist())
        return_df = return_df.drop_duplicates(keep=keep)
        return return_df


# ------------------------------------------------------------------------------
# `set_diff`
# ------------------------------------------------------------------------------


@pipe
def set_diff(df, other, index=False, keep='first'):
    """
    Returns rows that appear in the first DataFrame but not the second.

    Args:
        df (pandas.DataFrame): data passed in through the pipe.
        other (pandas.DataFrame): other DataFrame to use for set operation with
            the first.

    Kwargs:
        index (bool): Boolean indicating whether to consider the pandas index
            as part of the set operation (default `False`).
        keep (str): Indicates which duplicate should be kept. Options are `'first'`
            and `'last'`.
    """

    validate_set_ops(df, other)
    if index:
        df_reset_index = df.reset_index()
        other_reset_index = other.reset_index()
        index_cols = [col for col in df_reset_index.columns if col not in df.columns]
        df_index_names = df.index.names
        return_df = (pd.merge(df_reset_index, other_reset_index,
                              how='left',
                              left_on=df_reset_index.columns.values.tolist(),
                              right_on=other_reset_index.columns.values.tolist(),
                              indicator=True)
                     .set_index(index_cols))
        return_df = return_df[return_df._merge == 'left_only']
        return_df.index.names = df_index_names
        return_df = return_df.drop_duplicates(keep=keep)[df.columns]
        return return_df
    else:
        return_df = pd.merge(df, other,
                             how='left',
                             left_on=df.columns.values.tolist(),
                             right_on=df.columns.values.tolist(),
                             indicator=True)
        return_df = return_df[return_df._merge == 'left_only']
        return_df = return_df.drop_duplicates(keep=keep)[df.columns]
        return return_df
