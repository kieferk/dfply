from .vendor.pandas_ply import symbolic
from .vendor.pandas_ply.symbolic import X

import pandas as pd
from decorator import decorator


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
            kwargs = self._kwargs_eval(args[0], args[1:])

        return self.function(*args, **kwargs)



def _ambiguous_index_parser(df, indices):
    selected = []
    for index in indices:
        index = symbolic.to_callable(index)(df)
        assert type(index) in (str, int, list, tuple,
                               pd.Series, pd.DataFrame)
        if type(index) in (list, tuple):
            selected.extend(ambiguous_index_parser(df, index))
        elif type(index) in [int, str]:
            selected.append(index)
        elif type(index) == pd.Series:
            selected.append(index._name)
        elif type(index) == pd.DataFrame:
            indices = index.columns.values.tolist()
            selected.extend(indices)

    return selected


@decorator
def indexer_arguments(f, *args, **kwargs):
    assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
    if len(args) > 1:
        args = list(args[0:1]) + _ambiguous_index_parser(args[0], args[1:])
    if len(kwargs) > 0:
        kwargs = {k:_ambiguous_index_parser(args[0], v) for k,v in kwargs.items()}
    return f(*args, **kwargs)


def dfpipe(f):
    return Pipe(
        SymbolicEvaluation(
            GroupDelegation(f)
        )
    )
