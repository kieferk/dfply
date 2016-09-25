from .vendor.pandas_ply import symbolic
from .vendor.pandas_ply.symbolic import X

import pandas as pd
from decorator import decorator
import numpy as np
import warnings



# Initialize the global X symbol
X(0)

class Pipe(object):
    """
    Generic pipe decorator class that allows DataFrames to be passed
    through the __rrshift__ binary operator: >>

    data >> head(5)
    """

    __name__ = "Pipe"

    def __init__(self, function, depth=0):
        self.function = function

    def __rrshift__(self, other):
        return self.function(other)

    def __call__(self, *args, **kwargs):
        return Pipe(lambda x: self.function(x, *args, **kwargs))


class GroupDelegation(object):
    """
    Decorator that manages operation on groupings for data.

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
    """
    Decorator that evaluates symbolic objects from the pandas-ply
    symbolic module.

    Typically the only symbolic object passed through the pipes
    will be `X`. This is initialized as a global variable.

    diamonds >> select(X.carat, X.cut) >> head(2)

    Evaluation is done through with `symbolic.to_callable` function
    on both arguments and keyword argument values. The result is
    passed through to the decorated function.
    """

    __name__ = "SymbolicEvaluation"

    def __init__(self, function):
        self.function = function

    def _args_eval(self, df, args):
        args = [symbolic.to_callable(arg)(df) for arg in args]
        return [df]+args

    def _kwargs_eval(self, df, kwargs):
        kwargs = {k:symbolic.to_callable(v)(df) for k,v in kwargs.items()}
        return kwargs

    def __call__(self, *args, **kwargs):
        assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))

        if len(args) > 1:
            args = self._args_eval(args[0], args[1:])

        if len(kwargs) > 0:
            kwargs = self._kwargs_eval(args[0], kwargs)

        return self.function(*args, **kwargs)



def dfpipe(f):
    return Pipe(
        SymbolicEvaluation(
            GroupDelegation(f)
        )
    )



# ------------------------------------------------------------------------------
# Positional argument helpers and decorators
# ------------------------------------------------------------------------------


def _ambiguous_reference(df, ref):
    ref = symbolic.to_callable(ref)(df)
    if type(ref) == pd.Series:
        return ref._name
    else:
        return ref


def _ambiguous_reference_arg_parser(df, args):
    parsed = []
    for arg in args:
        arg = symbolic.to_callable(arg)(df)
        assert type(arg) in (str, int, list, tuple,
                               pd.Series, pd.DataFrame)
        if type(arg) in (list, tuple):
            parsed.extend(_ambiguous_reference_arg_parser(df, arg))
        elif type(arg) in [int, str]:
            parsed.append(arg)
        elif type(arg) == pd.Series:
            parsed.append(arg._name)
        elif type(arg) == pd.DataFrame:
            column_names = arg.columns.values.tolist()
            parsed.extend(column_names)
    return parsed


@decorator
def reference_args(f, *args, **kwargs):
    assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
    if len(args) > 1:
        args = list(args[0:1]) + _ambiguous_reference_arg_parser(args[0], args[1:])
    return f(*args, **kwargs)


@decorator
def reference_kwargs(f, *args, **kwargs):
    assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
    if len(kwargs) > 0:
        kwargs = {k:_ambiguous_reference(args[0], v) for k,v in kwargs.items()}
    return f(*args, **kwargs)


@decorator
def mixed_labels_to_integer(f, *args, **kwargs):
    assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
    columns = args[0].columns.tolist()
    positions = []
    for ind in args[1:]:
        if type(ind) == str:
            if ind not in columns:
                raise IndexError(str(ind)+' is not in columns.')
            else:
                positions.append(columns.index(ind))
        else:
            if ind < 0:
                raise IndexError(str(index)+' is negative. Not currently allowed.')
            positions.append(ind)
    args = list(args[0:1]) + positions
    return f(*args, **kwargs)


@decorator
def mixed_labels_to_string(f, *args, **kwargs):
    assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
    columns = args[0].columns.tolist()
    indices = []
    for ind in args[1:]:
        if type(ind) == str:
            indices.append(ind)
        else:
            raise warnings.warn('Integer indices will be inferred as column label positions.')
            if ind < 0:
                raise IndexError(str(index)+' is negative. Not currently allowed.')
            elif ind >= len(columns):
                raise IndexError(str(index)+' is greater than length of columns. Not allowed when inferred as column label positions.')
            else:
                indices.append(columns[ind])
    args = list(args[0:1]) + indices
    return f(*args, **kwargs)



def args_are_labels(f):
    return Pipe(
        SymbolicEvaluation(
            GroupDelegation(
                reference_args(
                    mixed_labels_to_string(f)
                    )
                )
            )
        )


def kwargs_are_labels(f):
    return Pipe(
        SymbolicEvaluation(
            GroupDelegation(
                reference_kwargs(f)
                )
            )
        )
