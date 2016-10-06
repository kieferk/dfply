from .vendor.pandas_ply import symbolic
from .vendor.pandas_ply.symbolic import X
from .vendor.six.moves import reduce
from .vendor.six import wraps

import pandas as pd
import numpy as np
import warnings


# Initialize the global X symbol
X(0)

class pipe(object):
    """
    Generic pipe decorator class that allows DataFrames to be passed
    through the __rrshift__ binary operator, >>
    """

    __name__ = "pipe"

    def __init__(self, function, depth=0):
        self.function = function

    def __rrshift__(self, other):
        return self.function(other)

    def __call__(self, *args, **kwargs):
        return pipe(lambda x: self.function(x, *args, **kwargs))


class group_delegation(object):
    """Decorator class that managing grouped operations on DataFrames.

    Checks for an attached `df._grouped_by` attribute added to a
    pandas DataFrame by the `groupby` function.

    If groups are found, the operation defined by the funciton is
    carried out for each group individually. The internal
    `_apply_combine_reset` function ensures that hierarchical
    indexing is removed.
    """

    __name__ = "group_delegation"

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
            df_copy = df.copy()
            df_copy = df_copy.groupby(grouped_by)

            try:
                assert self.function.function.__name__ == 'transmute'
                pass_args = grouped_by
            except:
                pass_args = args[1:]

            df_copy = self._apply_combine_reset(df_copy, *pass_args, **kwargs)
            if all([True if group in df.columns else False for group in grouped_by]):
                df_copy._grouped_by = grouped_by
            else:
                warnings.warn('Grouping lost due to transformation.')
            return df_copy

        else:
            return self.function(*args, **kwargs)



class symbolic_evaluation(object):
    """Decorator class that evaluates symbolic arguments and keyword arguments
    passed through to the decorated function.
    """

    __name__ = "symbolic_evaluation"

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



class symbolic_reference(object):
    """Decorator class that converts symbolic arguments and keyword arguments
    into their names (specifically `pandas.Series` objects)."""

    __name__ = "symbolic_reference"

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
    """Extracts arguments from lists or tuples and returns them
    "flattened".
    """
    flat = []
    for arg in args:
        if isinstance(arg, (list, tuple)):
            flat.extend(_arg_extractor(arg))
        else:
            flat.append(arg)
    return flat


def flatten_arguments(f):
    """Decorator that "flattens" any arguments contained inside of lists or
    tuples. Designed primarily for selection and dropping functions.

    Example:
        args = (a, b, (c, d, [e, f, g]))
        becomes
        args = (a, b, c, d, e, f, g)
    """
    @wraps(f)
    def wrapped(*args, **kwargs):
        flat_args = _arg_extractor(args)
        return f(*flat_args, **kwargs)
    return wrapped


def join_index_arguments(f):
    """Decorator for joining indexing arguments together. Designed primarily for
    `row_slice` to combine arbitrary single indices and lists of indices
    together.

    Example:
        args = (1, 2, 3, [4, 5], [6, 7])
        becomes
        args = ([1, 2, 3, 4, 5, 6, 7])
    """

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
    """Decorator that convertes column indicies to label. Typically decoration
    occurs after decoration by SymbolicReference.
    """
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
    """Decorator that converts column indices to integer position. Typically
    decoration occurs after decoration by SymbolicReference.
    """
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
    """Convenience chain of decorators for functions that operate with the
    expectation of having column labels as arguments (despite user potentially
    providing symbolic `pandas.Series` objects or integer column positions).
    """
    return pipe(
        symbolic_reference(
            flatten_arguments(
                column_indices_as_labels(f)
            )
        )
    )


def positional_selection(f):
    """Convenience chain of decorators for functions that operate with the
    expectation of having column integer positions as arguments (despite
    user potentially providing symbolic `pandas.Series` objects or column labels).
    """
    return pipe(
        symbolic_reference(
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
    return pipe(
        group_delegation(
            symbolic_evaluation(f)
        )
    )
