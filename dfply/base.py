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
    through the __rrshift__ binary operator: >>

    data >> head(5)
    """

    __name__ = "Pipe"

    def __init__(self, function, depth=0):
        self.function = function

    def __rrshift__(self, other):
        return self.function(other)

    def __call__(self, *args, **kwargs):
        #print 'Pipe', args, kwargs
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

    __name__ = "SymbolicEvaluation"

    def __init__(self, arg_labels=False, kwarg_labels=False,
                 labels_to_positional=False, positional_to_labels=False,
                 flatten_args=False):
        if labels_to_positional and positional_to_labels:
            raise Exception('Can only specify one conversion direction between labels and positional.')
        self.arg_labels = arg_labels
        self.kwarg_labels = kwarg_labels
        self.labels_to_positional = labels_to_positional
        self.positional_to_labels = positional_to_labels
        self.flatten_args = flatten_args


    def _label_or_arg(self, df, arg):
        arg = symbolic.to_callable(arg)(df)
        if isinstance(arg, pd.Series):
            return arg._name
        else:
            return arg


    def _label_to_integer(self, columns, label):
        if isinstance(label, str):
            if label not in columns:
                raise Exception("String label "+str(label)+' is not in columns.')
            return columns.index(label)
        elif isinstance(label, int):
            if label < 0:
                raise Exception("Int label "+str(label)+' is negative. Not currently allowed.')
            return label
        else:
            raise Exception("Label not of type str or int.")


    def _label_to_string(self, columns, label):
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


    def _ambiguous_selection_arg_flattener(self, df, args):
        parsed = []
        for arg in args:
            arg = symbolic.to_callable(arg)(df)
            assert type(arg) in (str, int, list, tuple,
                                 pd.Series, pd.DataFrame)
            if type(arg) in (list, tuple):
                parsed.extend(self._ambiguous_selection_arg_flattener(df, arg))
            elif type(arg) in [int, str]:
                parsed.append(arg)
            elif type(arg) == pd.Series:
                parsed.append(arg._name)
            elif type(arg) == pd.DataFrame:
                column_names = arg.columns.values.tolist()
                parsed.extend(column_names)
        return parsed


    def _args_eval(self, df, args):
        if self.arg_labels and self.flatten_args:
            args = self._ambiguous_selection_arg_flattener(df, args)
        elif self.arg_labels:
            args = [self._label_or_arg(df, arg) for arg in args]
        else:
            args = [symbolic.to_callable(arg)(df) for arg in args]

        if self.labels_to_positional:
            return [df]+[self._label_to_integer(df.columns.tolist(), arg) for arg in args]
        elif self.positional_to_labels:
            return [df]+[self._label_to_string(df.columns.tolist(), arg) for arg in args]
        else:
            return [df]+args


    def _kwargs_eval(self, df, kwargs):
        if self.kwarg_labels:
            kwargs = {k:self._label_or_arg(df, v) for k,v in kwargs.items()}
        else:
            kwargs = {k:symbolic.to_callable(v)(df) for k,v in kwargs.items()}
        return kwargs



    def __call__(self, f, *args, **kwargs):

        def fwrapped(*args, **kwargs):
            assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))

            if len(args) > 1:
                args = self._args_eval(args[0], args[1:])

            if len(kwargs) > 0:
                kwargs = self._kwargs_eval(args[0], kwargs)

            return f(*args, **kwargs)

        return fwrapped



# class SymbolicEvaluation(object):
#     """
#     Decorator that evaluates symbolic objects from the pandas-ply
#     symbolic module.
#
#     Typically the only symbolic object passed through the pipes
#     will be `X`. This is initialized as a global variable.
#
#     diamonds >> select(X.carat, X.cut) >> head(2)
#
#     Evaluation is done through with `symbolic.to_callable` function
#     on both arguments and keyword argument values. The result is
#     passed through to the decorated function.
#     """
#
#     __name__ = "SymbolicEvaluation"
#
#     def __init__(self, function):
#         self.function = function
#
#     def _args_eval(self, df, args):
#         args = [symbolic.to_callable(arg)(df) for arg in args]
#         return [df]+args
#
#     def _kwargs_eval(self, df, kwargs):
#         kwargs = {k:symbolic.to_callable(v)(df) for k,v in kwargs.items()}
#         return kwargs
#
#     def __call__(self, *args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#
#         if len(args) > 1:
#             args = self._args_eval(args[0], args[1:])
#
#         if len(kwargs) > 0:
#             kwargs = self._kwargs_eval(args[0], kwargs)
#
#         #print 'SymbolicEvaluation', args, kwargs
#         return self.function(*args, **kwargs)







# class SymbolicReference(object):
#
#     __name__ = "SymbolicReference"
#
#     def __init__(self, function):
#         self.function = function
#
#     def _args_eval(self, df, args):
#         args = [_ambiguous_reference(df, arg) for arg in args]
#         return [df]+args
#
#     def _kwargs_eval(self, df, kwargs):
#         kwargs = {k:_ambigious_reference(df, v) for k,v in kwargs.items()}
#         return kwargs
#
#     def __call__(self, *args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#
#         if len(args) > 1:
#             args = self._args_eval(args[0], args[1:])
#
#         if len(kwargs) > 0:
#             kwargs = self._kwargs_eval(args[0], kwargs)
#
#         return self.function(*args, **kwargs)


def dfpipe(f):
    return Pipe(
        GroupDelegation(
            SymbolicEvaluation()(f)
        )
    )



# ------------------------------------------------------------------------------
# Positional argument helpers and decorators
# ------------------------------------------------------------------------------

#
# def _ambiguous_reference_arg_parser(df, args):
#     parsed = []
#     for arg in args:
#         arg = symbolic.to_callable(arg)(df)
#         #print type(arg), arg
#         assert type(arg) in (str, int, list, tuple,
#                              pd.Series, pd.DataFrame)
#         if type(arg) in (list, tuple):
#             parsed.extend(_ambiguous_reference_arg_parser(df, arg))
#         elif type(arg) in [int, str]:
#             parsed.append(arg)
#         elif type(arg) == pd.Series:
#             parsed.append(arg._name)
#         elif type(arg) == pd.DataFrame:
#             column_names = arg.columns.values.tolist()
#             parsed.extend(column_names)
#     return parsed
#
#
# def reference_args(f):
#     @wraps(f)
#     def ref_args_wrapped(*args, **kwargs):
#         #print 'reference_args', args, kwargs
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         if len(args) > 1:
#             args = list(args[0:1]) + _ambiguous_reference_arg_parser(args[0], args[1:])
#         return f(*args, **kwargs)
#     return ref_args_wrapped
#
#
#
# def reference_kwargs(f):
#     @wraps(f)
#     def ref_kwargs_wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         if len(kwargs) > 0:
#             kwargs = {k:_ambiguous_reference(args[0], v) for k,v in kwargs.items()}
#         return f(*args, **kwargs)
#     return ref_kwargs_wrapped
#
#
# def _label_to_integer(columns, label):
#     if type(label) == str:
#         if label not in columns:
#             raise Exception("String label "+str(label)+' is not in columns.')
#         else:
#             return columns.index(label)
#     elif type(label) == int:
#         if label < 0:
#             raise Exception("Int label "+str(label)+' is negative. Not currently allowed.')
#         else:
#             return label
#     else:
#         raise Exception("Label not of type str or int.")
#
#
# def _label_to_string(columns, label):
#     if type(label) == str:
#         return label
#     elif type(label) == int:
#         warnings.warn('Int labels will be inferred as column positions.')
#         if label < 0:
#             raise Exception(str(label)+' is negative. Not currently allowed.')
#         elif label >= len(columns):
#             raise Exception(str(label)+' is greater than length of columns.')
#         else:
#             return columns[label]
#     else:
#         raise Exception("Label not of type str or int.")
#
#
# def arg_labels_to_integer(f):
#     @wraps(f)
#     def arg_labels_to_integer_wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         columns = args[0].columns.tolist()
#         positions = []
#         for ind in args[1:]:
#             positions.append(_label_to_integer(columns, ind))
#         args = list(args[0:1]) + positions
#         return f(*args, **kwargs)
#     return arg_labels_to_integer_wrapped
#
#
# def arg_labels_to_string(f):
#     @wraps(f)
#     def arg_labels_to_string_wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         columns = args[0].columns.tolist()
#         indices = []
#         for ind in args[1:]:
#             indices.append(_label_to_string(columns, ind))
#         args = list(args[0:1]) + indices
#         return f(*args, **kwargs)
#     return arg_labels_to_string_wrapped
#
#
# def kwarg_labels_to_integer(f):
#     @wraps(f)
#     def kwarg_labels_to_integer_wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         columns = args[0].columns.tolist()
#         positions = {}
#         for key, ind in kwargs.items():
#             positions[key] = _label_to_integer(columns, ind)
#         return f(*args, **positions)
#     return kwarg_labels_to_integer_wrapped
#
#
# def kwarg_labels_to_string(f):
#     @wraps(f)
#     def kwarg_labels_to_string_wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         columns = args[0].columns.tolist()
#         indices = {}
#         for key, ind in kwargs.items():
#             indices[key] = _label_to_string(columns, ind)
#         return f(*args, **indices)
#     return kwarg_labels_to_string_wrapped
#
#
#
# def args_reference_columns(f):
#     return Pipe(
#         GroupDelegation(
#             SymbolicEvaluation(
#                 reference_args(
#                     arg_labels_to_string(f)
#                     )
#                 )
#             )
#         )
#
#
# def kwargs_reference_columns(f):
#     return Pipe(
#         GroupDelegation(
#             SymbolicEvaluation(
#                 reference_kwargs(
#                     kwarg_labels_to_string(f)
#                     )
#                 )
#             )
#         )
