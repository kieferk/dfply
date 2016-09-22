from .base import *
import warnings
from decorator import decorator
import pandas as pd


# NOTE: selection currently removes groupings
# before performing the select, as if the grouping
# was not there. Is this consistent with best
# practices for table operations? Basically an
# ungroup() call is implicit.
def selectionpipe(f):
    return Pipe(
        SymbolicEvaluation(
            indexer_arguments(f)
        )
    )


def _index_missing_error(index):
    raise IndexError(str(index)+' is not in columns.')

def _negative_index_error(index):
    raise IndexError(str(index)+' is negative. Not currently allowed.')

def _int_index_greater_warning(index):
    raise warnings.warn(str(index)+' is greater than column length.')


@decorator
def indexer_arguments(f, *args, **kwargs):
    assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
    if len(args) > 1:
        args = list(args[0:1]) + _ambiguous_index_parser(args[0], args[1:])
    if len(kwargs) > 0:
        kwargs = {k:_ambiguous_index_parser(args[0], v) for k,v in kwargs.items()}
    return f(*args, **kwargs)


@decorator
def indices_to_positional(f, *args, **kwargs):
    assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
    columns = args[0].columns.tolist()
    positions = []
    for ind in args:
        if type(ind) == str:
            if ind not in columns:
                _index_missing_error(ind)
            else:
                positions.append(columns.index(ind))
        else:
            if ind < 0:
                _negative_index_error(ind)
            positions.append(ind)
    args = list(args[0:1]) + positions
    return f(*args, **kwargs)


def _indices_to_positions(columns, indices):
    positions = []
    for ind in indices:
        if type(ind) == str:
            if ind not in columns:
                _index_missing_error(ind)
            else:
                positions.append(columns.index(ind))
        else:
            if ind < 0:
                _negative_index_error(ind)
            positions.append(ind)
    return positions



@selectionpipe
@indices_to_positional
def select(df, *args):
    #positions = _indices_to_positions(df.columns.tolist(), args)
    #selected = df.columns[positions]
    return df[df.columns[list(args)]]


@selectionpipe
def drop(df, *args):
    positions = _indices_to_positions(df.columns.tolist(), args)
    selected = df.columns[positions]
    return df.drop(selected, axis=1)


@selectionpipe
def select_containing(df, *args):
    column_matches = [
        col for col in df.columns if any([(x in col) for x in args])
    ]
    return df[column_matches]


@selectionpipe
def drop_containing(df, *args):
    column_matches = [
        col for col in df.columns if any([(x in col) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@selectionpipe
def select_startswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.startswith(x) for x in args])
    ]
    return df[column_matches]


@selectionpipe
def drop_startswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.startswith(x) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@selectionpipe
def select_endswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.endswith(x) for x in args])
    ]
    return df[column_matches]


@selectionpipe
def drop_endswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.endswith(x) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@selectionpipe
def select_between(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        raise IndexError('No indices supplied to select_between.')
    elif len(args) == 1:
        warnings.warn("select_between with single index defaults to select_from.")

    positions = _indices_to_positions(columns, args)
    ind1 = positions[0]
    ind2 = positions[1] if len(positions) > 1 else len(columns)
    return df[columns[ind1:ind2+1]]


@selectionpipe
def drop_between(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        raise IndexError('No indices supplied to drop_between.')
    elif len(args) == 1:
        warnings.warn("drop_between with single index defaults to drop_from.")

    positions = _indices_to_positions(columns, args)
    ind1 = positions[0]
    ind2 = positions[1] if len(positions) > 1 else len(columns)
    return df.drop(columns[ind1:ind2+1], axis=1)


@selectionpipe
def select_from(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        raise IndexError('No index supplied to select_from.')
    positions = _indices_to_positions(columns, args)
    ind = positions[0]
    return df[columns[ind:]]


@selectionpipe
def drop_from(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        raise IndexError('No index supplied to drop_from.')
    positions = _indices_to_positions(columns, args)
    ind = positions[0]
    return df.drop(columns[ind:], axis=1)


@selectionpipe
def select_to(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        raise IndexError('No index supplied to select_to.')
    positions = _indices_to_positions(columns, args)
    ind = positions[0]
    return df[columns[:ind]]


@selectionpipe
def drop_to(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        raise IndexError('No index supplied to drop_to.')
    positions = _indices_to_positions(columns, args)
    ind = positions[0]
    return df.drop(columns[:ind], axis=1)


@selectionpipe
def select_through(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        raise IndexError('No index supplied to select_through.')
    positions = _indices_to_positions(columns, args)
    ind = positions[0]
    return df[columns[:ind+1]]


@selectionpipe
def drop_through(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        raise IndexError('No index supplied to drop_through.')
    positions = _indices_to_positions(columns, args)
    ind = positions[0]
    return df.drop(columns[:ind+1], axis=1)
