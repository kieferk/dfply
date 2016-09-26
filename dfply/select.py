from .base import *
import warnings


# ==============================================================================
#
# SELECTORS
#
# These functions are for subsetting along columns (axis=1).
#
# NOTE: Currently selection removes groupings as indicated in the `._grouped_by`
# attribute attached by the `groupby()` function. I'm still not sure whether
# I like this or not, but a select operation on groups prior to an aggregation
# function is atypical from the start, so I'm not sure if there is a "best
# practice" here.
#
# ==============================================================================


def label_selection(f):
    return Pipe(
        SymbolicEvaluation(
            reference_args(
                arg_labels_to_string(f)
                )
            )
        )


def positional_selection(f):
    return Pipe(
        SymbolicEvaluation(
            reference_args(
                arg_labels_to_integer(f)
                )
            )
        )


# ------------------------------------------------------------------------------
# Select and drop operators
# ------------------------------------------------------------------------------

@positional_selection
def select(df, *args):
    return df[df.columns[list(args)]]


@positional_selection
def drop(df, *args):
    columns = [col for i, col in enumerate(df.columns) if not i in args]
    return df[columns]


@label_selection
def select_containing(df, *args):
    column_matches = [
        col for col in df.columns if any([(x in col) for x in args])
    ]
    return df[column_matches]


@label_selection
def drop_containing(df, *args):
    column_matches = [
        col for col in df.columns if any([(x in col) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@label_selection
def select_startswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.startswith(x) for x in args])
    ]
    return df[column_matches]


@label_selection
def drop_startswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.startswith(x) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@label_selection
def select_endswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.endswith(x) for x in args])
    ]
    return df[column_matches]


@label_selection
def drop_endswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.endswith(x) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@positional_selection
def select_between(df, start_index, end_index):
    return df[df.columns[start_index:end_index+1]]


@positional_selection
def drop_between(df, start_index, end_index):
    return df.drop(df.columns[start_index:end_index+1], axis=1)


@positional_selection
def select_from(df, start_index):
    return df[df.columns[start_index:]]


@positional_selection
def drop_from(df, start_index):
    return df.drop(df.columns[start_index:], axis=1)


@positional_selection
def select_to(df, end_index):
    return df[df.columns[:end_index]]


@positional_selection
def drop_to(df, end_index):
    return df.drop(df.columns[:end_index], axis=1)


@positional_selection
def select_through(df, end_index):
    return df[df.columns[:end_index+1]]


@positional_selection
def drop_through(df, end_index):
    return df.drop(df.columns[:end_index+1], axis=1)
