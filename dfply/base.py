from .vendor.pandas_ply import symbolic
from .vendor.pandas_ply.symbolic import X
from .vendor.six import wraps

import pandas as pd
import numpy as np
import warnings


# Initialize the global X symbol
X(0)

class Pipe(object):
    """
    Generic pipe decorator class that allows DataFrames to be passed
    through the __rrshift__ binary operator, >>
    """

    __name__ = "Pipe"

    def __init__(self, function, depth=0):
        self.function = function

    def __rrshift__(self, other):
        return self.function(other)

    def __call__(self, *args, **kwargs):
        return Pipe(lambda x: self.function(x, *args, **kwargs))


class GroupDelegation(object):
    """Decorator class that managing grouped operations on DataFrames.

    Checks for an attached `df._grouped_by` attribute added to a
    pandas DataFrame by the `groupby` function.

    If groups are found, the operation defined by the funciton is
    carried out for each group individually. The internal
    `_apply_combine_reset` function ensures that hierarchical
    indexing is removed.
    """

    __name__ = "GroupDelegation"

    def __init__(self, function):
        self.function = function


    def _apply_combine_reset(self, grouped, *args, **kwargs):
        combined = grouped.apply(self.function, *args, **kwargs)

        for name in combined.index.names[:-1]:
            if name in combined:
                combined.reset_index(level=0, drop=True, inplace=True)
            else:
                combined.reset_index(level=0, inplace=True)

        if (combined.index == 0).all():
            combined.reset_index(drop=True, inplace=True)

        return combined


    def __call__(self, *args, **kwargs):
        assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))

        df = args[0]
        grouped_by = getattr(df, "_grouped_by", None)

        if grouped_by is not None:
            df = df.groupby(grouped_by)
            df = self._apply_combine_reset(df, *args[1:], **kwargs)
            df._grouped_by = grouped_by
            return df

        else:
            return self.function(*args, **kwargs)



class SymbolicEvaluation(object):

    __name__ = "SymbolicEvaluation"

    def __init__(self, function):
        self.function = function

    def _args_eval(self, df, args):
        return [df]+[symbolic.to_callable(arg)(df) for arg in args]

    def _kwargs_eval(self, df, kwargs):
        return {k:symbolic.to_callable(v)(df) for k,v in kwargs.items()}

    def __call__(self, *args, **kwargs):
        assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
        if len(args) > 1:
            args = self._args_eval(args[0], args[1:])
        if len(kwargs) > 0:
            kwargs = self._kwargs_eval(args[0], kwargs)
        return self.function(*args, **kwargs)



class SymbolicReference(object):

    __name__ = "SymbolicReference"

    def __init__(self, function):
        self.function = function

    def _label_or_arg(self, df, arg):
        """Recursively extracts pandas series arguments or retains original
        argument."""
        arg = symbolic.to_callable(arg)(df)
        if isinstance(arg, pd.Series):
            return arg._name
        elif isinstance(arg, pd.DataFrame):
            return arg.columns.tolist()
        elif isinstance(arg, (list, tuple)):
            return [self._label_or_arg(df, subarg) for subarg in arg]
        else:
            return arg

    def _args_eval(self, df, args):
        return [df]+[self._label_or_arg(df, arg) for arg in args]

    def _kwargs_eval(self, df, kwargs):
        return {k:self._label_or_arg(df, v) for k,v in kwargs.items()}

    def __call__(self, *args, **kwargs):
        assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
        if len(args) > 1:
            args = self._args_eval(args[0], args[1:])
        if len(kwargs) > 0:
            kwargs = self._kwargs_eval(args[0], kwargs)
        return self.function(*args, **kwargs)



def _arg_extractor(args):
    flat = []
    for arg in args:
        if isinstance(arg, (list, tuple)):
            flat.extend(_arg_extractor(arg))
        else:
            flat.append(arg)
    return flat


def flatten_arguments(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        flat_args = _arg_extractor(args)
        return f(*flat_args, **kwargs)
    return wrapped


def join_index_arguments(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
        if len(args) > 1:
            args_ = reduce(lambda x, y: np.concatenate([np.atleast_1d(x), np.atleast_1d(y)]),
                           args[1:])
            args = [args[0]] + [np.atleast_1d(args_)]
        return f(*args, **kwargs)
    return wrapped



def _col_ind_to_position(columns, ind):
    """Converts column indexers to their integer position."""
    if isinstance(ind, str):
        if ind not in columns:
            raise Exception("String label "+str(ind)+' is not in columns.')
        return columns.index(ind)
    elif isinstance(ind, int):
        if ind < 0:
            raise Exception("Int label "+str(ind)+' is negative. Not currently allowed.')
        return ind
    else:
        raise Exception("Column indexer not of type str or int.")



def _col_ind_to_label(columns, label):
    """Converts column indexers positions to their string label."""
    if isinstance(label, str):
        return label
    elif isinstance(label, int):
        warnings.warn('Int labels will be inferred as column positions.')
        if label < 0:
            raise Exception(str(label)+' is negative. Not currently allowed.')
        elif label >= len(columns):
            raise Exception(str(label)+' is greater than length of columns.')
        else:
            return columns[label]
    else:
        raise Exception("Label not of type str or int.")


def column_indices_as_labels(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
        if len(args) > 1:
            label_args = [_col_ind_to_label(args[0].columns.tolist(), arg)
                          for arg in args[1:]]
            args = [args[0]]+label_args
        return f(*args, **kwargs)
    return wrapped


def column_indices_as_positions(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
        if len(args) > 1:
            label_args = [_col_ind_to_position(args[0].columns.tolist(), arg)
                          for arg in args[1:]]
            args = [args[0]]+label_args
        return f(*args, **kwargs)
    return wrapped



def label_selection(f):
    return Pipe(
        SymbolicReference(
            flatten_arguments(
                column_indices_as_labels(f)
            )
        )
    )


def positional_selection(f):
    return Pipe(
        SymbolicReference(
            flatten_arguments(
                column_indices_as_positions(f)
            )
        )
    )



def dfpipe(f):
    """Standard chain of decorators for a function to be used with dfply.
    The function can be chained with >> by `Pipe`, application of the function
    to grouped DataFrames is enabled by `GroupDelegation`, and symbolic
    arguments are evaluated as-is using a default `SymbolicEvaluation`."""
    return Pipe(
        GroupDelegation(
            SymbolicEvaluation(f)
        )
    )
