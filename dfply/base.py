from __future__ import absolute_import

#from pandas_ply import symbolic
from . import symbolic

#from pandas_ply.symbolic import X
from .symbolic import X

from six.moves import reduce
from six import wraps

import functools
from functools import partial

import pandas as pd
import numpy as np
import warnings



class pipe(object):
    """Generic pipe decorator class that allows DataFrames to be passed
    through the `__rrshift__` binary operator, `>>`

    Adapted from:
    https://github.com/JulienPalard/Pipe/blob/master/pipe.py

    Where the two differences are the `>>` operator is used instead of the
    `|` operator, and DataFrame copying logic occurs in the operator
    overloader function.
    """

    __name__ = "pipe"


    def __init__(self, function):
        self.function = function


    def __rrshift__(self, other):
        other_copy = other.copy()
        other_copy._grouped_by = getattr(other, '_grouped_by', None)
        return self.function(other_copy)


    def __call__(self, *args, **kwargs):
        return pipe(lambda x: self.function(x, *args, **kwargs))



class group_delegation(object):
    """Decorator class that managing grouped operations on DataFrames.

    Checks for an attached `df._grouped_by` attribute added to a
    pandas DataFrame by the `groupby` function.

    If groups are found, the operation defined by the function is
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
            df = df.groupby(grouped_by)

            try:
                assert self.function.function.__name__ == 'transmute'
                pass_args = grouped_by
            except:
                pass_args = args[1:]

            df = self._apply_combine_reset(df, *pass_args, **kwargs)
            if all([True if group in df.columns else False for group in grouped_by]):
                df._grouped_by = grouped_by
            else:
                warnings.warn('Grouping lost during transformation.')
            return df

        else:
            return self.function(*args, **kwargs)


def function_map(function_list, x):
    if len(function_list) > 0:
        return reduce(lambda x, f: f(x), [x]+list(function_list))
    else:
        return x


def helper_function(f=None, **opt):
    if f is None:
        return partial(helper_function, **opt)

    if opt.get('delay_evaluation', False):
        handler = SymbolicHandler(embedded_function=True,
                                  delay_evaluation=True)
    else:
        handler = SymbolicHandler(embedded_function=True,
                                  delay_evaluation=False)
    return handler(f)



def dfpipe(f=None, **kwargs):
    if f is None:
        return partial(dfpipe, **kwargs)

    ignore_grouping = kwargs.get('ignore_grouping', False)
    ignore_symbolic = kwargs.get('ignore_symbolic', False)

    decorators = [pipe]
    if not ignore_grouping: decorators.append(group_delegation)
    if not ignore_symbolic:
        handler = SymbolicHandler(**kwargs)
        decorators.append(handler)

    decorators = decorators[::-1]
    return function_map(decorators, f)





class SymbolicHandler(object):
    """
    Parent class for symbolic argument decorators.

    Default behavior is to recursively turn the arguments and keyword
    arguments of a decorated function into `symbolic.Call` objects that
    can be evaluated against a pandas DataFrame as it comes down a pipe.
    """

    __name__ = "SymbolicHandler"
    args_have_symbolic = False
    attempt_embedded_eval = True
    df = {}
    columns = []

    arg_processors = []
    kwarg_processors = []
    invert_override = {'__invert__':lambda x: symbolic.to_callable(x)}

    def __init__(self, *args, **kwargs):

        self.function = None

        if args and callable(args[0]):
            # used as decorator without being called
            self.init()
            self.wraps(args[0])
        else:
            self.init(*args, **kwargs)


    def init(self, *args, **kwargs):
        # same structure as in Decorum:
        # https://github.com/zeekay/decorum/blob/master/decorum/decorum.py
        self.assigned = functools.WRAPPER_ASSIGNMENTS
        self.arg_processors = [self.recurse_args]
        self.kwarg_processors = [self.recurse_kwargs]

        embedded_function = kwargs.get('embedded_function', False)
        delayed_evaluation = kwargs.get('delay_evaluation', False)
        flatten_args = kwargs.get('flatten_args', False)
        selector = kwargs.get('selector', False)
        args_as_labels = kwargs.get('args_as_labels', False)
        kwargs_as_labels = kwargs.get('kwargs_as_labels', False)
        args_as_positional = kwargs.get('args_as_positional', False)
        kwargs_as_positional = kwargs.get('kwargs_as_positional', False)
        join_index_args = kwargs.get('join_index_args', False)

        if ((args_as_labels and args_as_positional) or
            (kwargs_as_labels and kwargs_as_positional)):
            raise Exception('Cannot specify conversion to both labels and positional.')

        if embedded_function and not delayed_evaluation:
            self.function_action = self.eval_function_no_context
        elif embedded_function and delayed_evaluation:
            pass

        #if flatten_args:
        #    self.arg_processors.append(self.flatten_arguments)

        else:
            self.function_action = self.eval_function_data_context
            self.call_init = self.call_init_data

            to_label = partial(_try_except, f_try=lambda x: list(self.columns)[x])
            to_position = partial(_try_except, f_try=lambda x: list(self.columns).index(x))

            if args_as_labels:
                self.arg_action = partial(_argument_symbolic_reference,
                                          switch_func=to_label)
            elif args_as_positional:
                self.arg_action = partial(_argument_symbolic_reference,
                                          switch_func=to_position)

            if kwargs_as_labels:
                self.kwarg_action = partial(_argument_symbolic_reference,
                                            switch_func=to_label)
            elif kwargs_as_positional:
                self.kwarg_action = partial(_argument_symbolic_reference,
                                            switch_func=to_position)

            if selector:
                self.arg_action = self.selector_arg_action
                self.arg_processors.append(self.selection_to_inds)
                if args_as_labels:
                    self.arg_processors.append(lambda x: [to_label(y) for y in x])
                if args_as_positional:
                    self.arg_processors.append(lambda x: [to_position(y) for y in x])


            if flatten_args:
                self.arg_processors.append(self.flatten_arguments)

            if join_index_args:
                self.arg_processors.append(self.join_indices)

            self.arg_processors.append(self.prepend_data)

        return self


    def wraps(self, f):
        self.function = f
        functools.update_wrapper(self, f, self.assigned or (), ())
        return self


    def __call__(self, *args, **kwargs):
        if self.function:
            return self.call(*args, **kwargs)
        else:
            return self.wraps(args[0])


    def arg_action(self, arg):
        return self.argument_symbolic_eval(arg)

    def kwarg_action(self, kwarg):
        return self.argument_symbolic_eval(kwarg)

    def function_action(self, function):
        return function

    def recurse_args(self, args):
        return [self.arg_action(arg) for arg in args]

    def recurse_kwargs(self, kwargs):
        return {k:self.kwarg_action(v) for k,v in kwargs.items()}

    def call_init(self, *args, **kwargs):
        return args, kwargs


    def argument_symbolic_eval(self, arg):
        if isinstance(arg, (list, tuple)):
            arglist = [self.general_symbolic_eval(subarg) for subarg in arg]
            return symbolic.sym_call(lambda *x: x, *arglist)
        else:
            if isinstance(arg, symbolic.Expression):
                self.args_have_symbolic = True
            return arg

    def argument_symbolic_reference(self, arg, switch_func=lambda x: x):
        if hasattr(arg, '_eval'):
            arg = symbolic.to_callable(arg)(self.df)
        if isinstance(arg, pd.Series):
            arg = switch_func(arg.name)
        elif isinstance(arg, pd.DataFrame):
            return symbolic.sym_call(lambda *x: x, arg.columns.tolist())
        elif isinstance(arg, (list, tuple, pd.Index)):
            arglist = [self.argument_symbolic_reference(subarg, switch_func=switch_func)
                       for subarg in arg]
            return symbolic.sym_call(lambda *x: x, *arglist)
        return switch_func(arg)


    # SELECTOR LOGIC FUNCTIONS:
    def _logical_selection_join(self, first, second):
        first, second = np.array(first), np.array(second)
        first[~np.isnan(second)] = second[~np.isnan(second)]
        return first


    def _count_inversions(self, arg, assignment):
        if isinstance(arg, symbolic.Expression):
            inversions = repr(arg).count("__invert__")
            if (inversions % 2) != 0:
                assignment = int(not assignment)
        return assignment


    def _override_invert_eval(self, arg):
        while callable(arg):
            arg = symbolic.to_callable(arg, override_attr=self.invert_override)
            arg = arg(self.df)
        return arg


    def _selection_from_list(self, arg, assignment, selection):
        selection = [self.selector_arg_action(arg_, assignment=assignment) for arg_ in arg]
        if len(arg) > 1:
            selection = reduce(self._logical_selection_join, selection)
        else:
            selection = selection[0]
        return selection


    def _selection_from_array(self, arg, assignment, selection):
        if arg.dtype == int:
            selection[arg[arg >= 0]] = assignment
            selection[~arg[arg < 0]] = int(not assignment)
        elif arg.dtype == bool:
            assert len(arg) == len(selection)
            selection = arg
        else:
            selection = [self.selector_arg_action(arg_, assignment=assignment) for arg_ in arg]
            if len(arg) > 1:
                selection = reduce(self._logical_selection_join, selection)
            elif len(arg) > 0:
                selection = selection[0]
        return selection


    def _selection_from_str(self, arg, assignment, selection):
        selection[self.columns.tolist().index(arg)] = assignment
        return selection


    def _selection_from_series(self, arg, assignment, selection):
        selection[self.columns.tolist().index(arg.name)] = assignment
        return selection


    def _selection_from_index(self, arg, assignment, selection):
        selection[[i for i,c in enumerate(self.columns) if c in arg]] = assignment
        return selection


    def _selection_from_int(self, arg, assignment, selection):
        if arg >= 0:
            selection[arg] = assignment
        else:
            selection[~arg] = int(not assignment)
        return selection


    def selector_arg_action(self, arg, assignment=1):
        selection = np.repeat(np.nan, len(self.columns))
        assignment = self._count_inversions(arg, assignment)
        arg = self._override_invert_eval(arg)

        if isinstance(arg, (list, tuple)):
            selection = self._selection_from_list(arg, assignment, selection)
        elif isinstance(arg, np.ndarray):
            selection = self._selection_from_array(arg, assignment, selection)
        elif isinstance(arg, str):
            selection = self._selection_from_str(arg, assignment, selection)
        elif isinstance(arg, pd.Series):
            selection = self._selection_from_series(arg, assignment, selection)
        elif isinstance(arg, (pd.DataFrame, pd.Index)):
            if isinstance(arg, pd.DataFrame):
                arg = arg.columns
            selection = self._selection_from_index(arg, assignment, selection)
        elif isinstance(arg, int):
            selection = self._selection_from_int(arg, assignment, selection)

        if len(selection) == 0:
            return np.zeros(len(self.columns))
        return selection


    def selection_to_inds(self, selection):
        any_positive = any([np.nansum(vec) > 0 for vec in selection])
        if not any_positive:
            selection = [np.ones(len(self.columns))]+selection
        if len(selection) > 1:
            selection = reduce(self._logical_selection_join, selection)
        else:
            selection = selection[0]
        selection[np.isnan(selection)] = 0
        #return [np.where(selection)[0]]
        return np.where(selection)[0]


    def flatten_arguments(self, args):
        flat = []
        for arg in args:
            #if hasattr(arg, '_eval'):
            #    arg = symbolic.to_callable(arg)(self.df)
            if isinstance(arg, (list, tuple, pd.Index)):
                flat.extend(self.flatten_arguments(arg))
            else:
                flat.append(arg)
        return flat


    def join_indices(self, index_args):
        if len(index_args) > 1:
            args_ = reduce(lambda x, y: np.concatenate([np.atleast_1d(x),
                                                        np.atleast_1d(y)]),
                           index_args)
            return [np.atleast_1d(args_)]
        else:
            return index_args



    def column_label_to_position(self, indexer):
        if isinstance(indexer, str):
            if indexer not in self.columns:
                raise Exception("String label "+str(indexer)+' is not in columns.')
            return self.columns.index(indexer)
        elif isinstance(indexer, int):
            return indexer
        else:
            raise Exception("Column indexer not of type str or int.")


    def column_position_to_label(self, indexer):
        if isinstance(indexer, str):
            return indexer
        elif isinstance(indexer, int):
            warnings.warn('Int labels will be inferred as column positions.')
            if indexer < -1*len(self.columns):
                raise Exception(str(indexer)+' is negative and less than length of columns.')
            elif indexer >= len(self.columns):
                raise Exception(str(indexer)+' is greater than length of columns.')
            else:
                return list(self.columns)[indexer]
        else:
            raise Exception("Label not of type str or int.")


    def positional_to_label(self, indices):
        if not isinstance(indices, dict):
            return [self.column_position_to_label(index) for index in indices]
        else:
            return {k:self.column_position_to_label(v) for k,v in indices.items()}


    def label_to_positional(self, indices):
        if not isinstance(indices, dict):
            return [self.column_label_to_position(index) for index in indices]
        else:
            return {k:self.column_label_to_position(v) for k,v in indices.items()}


    def eval_function_no_context(self, symbolic_function):
        if not self.args_have_symbolic:
            return symbolic.eval_if_symbolic(symbolic_function, {})
        else:
            return symbolic_function

    def eval_function_data_context(self, symbolic_function):
        return symbolic.to_callable(symbolic_function)(self.df)


    def call_init_data(self, *args, **kwargs):
        assert isinstance(args[0], pd.DataFrame)
        self.df = args[0]
        self.columns = self.df.columns
        return args[1:], kwargs


    def prepend_data(self, args):
        return [self.df]+list(args)


    def call(self, *args, **kwargs):
        args, kwargs = self.call_init(*args, **kwargs)
        args = function_map(self.arg_processors, args)
        kwargs = function_map(self.kwarg_processors, kwargs)

        symbolic_function = symbolic.Call(self.function, args=args, kwargs=kwargs)

        return self.function_action(symbolic_function)



### TO deprecate

# class eval_or_symbolic(SymbolicHandler):
#     """
#     A decorator that turns a function into a "delayed" function to be evaluated
#     only when it has access to the pandas DataFrame proceeding through the
#     pipe.
#
#     This decorator is primarily used to decorate functions that operate on
#     Series (columns), either as an argument within a function call or inside
#     of another function.
#
#     The `desc` function, for example, is decorated by @make_symbolic so that
#     it will wait to evaluate at the appropriate time.
#
#     Example:
#         diamonds >> arrange(desc(X.price), desc(X.carat)) >> head(5)
#     """
#
#     __name__ = "eval_or_symbolic"
#
#
#     def __init__(self, function):
#         super(eval_or_symbolic, self).__init__(function)
#
#
#     def call_action(self, args, kwargs):
#         symbolic_function = symbolic.Call(self.function,
#                                           args=self.recurse_args(args),
#                                           kwargs=self.recurse_kwargs(kwargs))
#
#         if not self.call_has_symbolic:
#             return symbolic.eval_if_symbolic(symbolic_function, {})
#         else:
#             return symbolic_function
#
#
#
# class symbolic_function(SymbolicHandler):
#
#     __name__ = "symbolic_function"
#
#     def __init__(self, function):
#         super(symbolic_function, self).__init__(function)
#
#     def call_action(self, args, kwargs):
#         return symbolic.Call(self.function,
#                              args=self.recurse_args(args),
#                              kwargs=self.recurse_kwargs(kwargs))
#
#
#
#
# class selection(SymbolicHandler):
#
#     __name__ == "selection"
#     columns = None
#     column_order = []
#     invert_override = {'__invert__':lambda x: symbolic.to_callable(x)}
#
#     def __init__(self, function):
#         super(selection, self).__init__(function)
#
#
#     def logical_selection_join(self, first, second):
#         first, second = np.array(first), np.array(second)
#         first[~np.isnan(second)] = second[~np.isnan(second)]
#         return first
#
#
#     def _count_inversions(self, arg, assignment):
#         if isinstance(arg, symbolic.Expression):
#             inversions = repr(arg).count("__invert__")
#             if (inversions % 2) != 0:
#                 assignment = int(not assignment)
#         return assignment
#
#
#     def _override_invert_eval(self, arg):
#         while callable(arg):
#             arg = symbolic.to_callable(arg, override_attr=self.invert_override)
#             arg = arg(self.df)
#         return arg
#
#
#     def _selection_from_list(self, arg, assignment, selection):
#         selection = [self.arg_action(arg_, assignment=assignment) for arg_ in arg]
#         if len(arg) > 1:
#             selection = reduce(self.logical_selection_join, selection)
#         else:
#             selection = selection[0]
#         return selection
#
#
#     def _selection_from_array(self, arg, assignment, selection):
#         if arg.dtype == int:
#             selection[arg[arg >= 0]] = assignment
#             selection[~arg[arg < 0]] = int(not assignment)
#         elif arg.dtype == bool:
#             assert len(arg) == len(selection)
#             selection = arg
#         else:
#             selection = [self.arg_action(arg_, assignment=assignment) for arg_ in arg]
#             if len(arg) > 1:
#                 selection = reduce(self.logical_selection_join, selection)
#             elif len(arg) > 0:
#                 selection = selection[0]
#         return selection
#
#
#     def _selection_from_str(self, arg, assignment, selection):
#         selection[self.columns.tolist().index(arg)] = assignment
#         return selection
#
#
#     def _selection_from_series(self, arg, assignment, selection):
#         selection[self.columns.tolist().index(arg.name)] = assignment
#         return selection
#
#     def _selection_from_index(self, arg, assignment, selection):
#         selection[[i for i,c in enumerate(self.columns) if c in arg]] = assignment
#         return selection
#
#
#     def _selection_from_int(self, arg, assignment, selection):
#         if arg >= 0:
#             selection[arg] = assignment
#         else:
#             selection[~arg] = int(not assignment)
#         return selection
#
#
#     def arg_action(self, arg, assignment=1):
#         selection = np.repeat(np.nan, len(self.columns))
#         assignment = self._count_inversions(arg, assignment)
#         arg = self._override_invert_eval(arg)
#
#         if isinstance(arg, (list, tuple)):
#             selection = self._selection_from_list(arg, assignment, selection)
#         elif isinstance(arg, np.ndarray):
#             selection = self._selection_from_array(arg, assignment, selection)
#         elif isinstance(arg, str):
#             selection = self._selection_from_str(arg, assignment, selection)
#         elif isinstance(arg, pd.Series):
#             selection = self._selection_from_series(arg, assignment, selection)
#         elif isinstance(arg, (pd.DataFrame, pd.Index)):
#             if isinstance(arg, pd.DataFrame):
#                 arg = arg.columns
#             selection = self._selection_from_index(arg, assignment, selection)
#         elif isinstance(arg, int):
#             selection = self._selection_from_int(arg, assignment, selection)
#
#         if len(selection) == 0:
#             return np.zeros(len(self.columns))
#         return selection
#
#
#     def kwarg_action(self, kwarg):
#         return self.argument_symbolic_eval(kwarg)
#
#
#     def recurse_args(self, args):
#         selection = [self.arg_action(arg) for arg in args]
#         any_positive = any([np.nansum(vec) > 0 for vec in selection])
#         if not any_positive:
#             selection = [np.ones(len(self.columns))]+selection
#         if len(selection) > 1:
#             selection = reduce(self.logical_selection_join, selection)
#         else:
#             selection = selection[0]
#         selection[np.isnan(selection)] = 0
#         return [np.where(selection)[0]]
#
#
#     def call_action(self, args, kwargs):
#         assert isinstance(args[0], pd.DataFrame)
#         self.df = args[0]
#         self.columns = self.df.columns
#
#         symbolic_function = symbolic.Call(self.function,
#                                           args=[args[0]]+self.recurse_args(args[1:]),
#                                           kwargs=self.recurse_kwargs(kwargs))
#
#         return symbolic.to_callable(symbolic_function)(self.df)
#
#
#
# class symbolic_evaluation(SymbolicHandler):
#     """
#     Decorates functions that may contain symbolic arguments or keyword
#     arguments, evaluating them against the pandas DataFrame in the pipe.
#     """
#
#     __name__ = "symbolic_evaluation"
#
#     def __init__(self, function):
#         super(symbolic_evaluation, self).__init__(function)
#
#
#     def arg_action(self, arg):
#         return self.argument_symbolic_eval(arg)
#
#
#     def kwarg_action(self, kwarg):
#         return self.argument_symbolic_eval(kwarg)
#
#
#     def call_action(self, args, kwargs):
#         symbolic_function = symbolic.Call(self.function,
#                                           args=self.recurse_args(args),
#                                           kwargs=self.recurse_kwargs(kwargs))
#         return symbolic.to_callable(symbolic_function)(args[0])
#
#
#
#
# class symbolic_reference(SymbolicHandler):
#     """
#     Similar to `symbolic_evaluation`, but instead of evaluating pandas objects
#     in their entirety, extracts the labels/names of the objects.
#
#     This is for convenience and primarily used to decorate the selection and
#     dropping functions.
#     """
#
#     __name__ = "symbolic_reference"
#
#     def __init__(self, function):
#         super(symbolic_reference, self).__init__(function)
#
#
#     def arg_action(self, arg):
#         return self.argument_symbolic_reference(arg)
#
#
#     def kwarg_action(self, kwarg):
#         return self.argument_symbolic_reference(kwarg)
#
#
#     def call_action(self, args, kwargs):
#         self.df = args[0]
#         symbolic_function = symbolic.Call(self.function,
#                                           args=[self.df]+self.recurse_args(args[1:]),
#                                           kwargs=self.recurse_kwargs(kwargs))
#         return symbolic.to_callable(symbolic_function)(self.df)
#
#
#
# class symbolic_reference_args(symbolic_reference):
#
#     __name__ = "symbolic_reference_args"
#
#
#     def __init__(self, function):
#         super(symbolic_reference_args, self).__init__(function)
#
#
#     def kwarg_action(self, kwarg):
#         return self.argument_symbolic_eval(kwarg)
#
#
#
# class symbolic_reference_kwargs(symbolic_reference):
#
#     __name__ = "symbolic_reference_kwargs"
#
#
#     def __init__(self, function):
#         super(symbolic_reference_kwargs, self).__init__(function)
#
#
#     def arg_action(self, arg):
#         return self.argument_symbolic_eval(arg)
#
#
#
#
# def _arg_extractor(args):
#     """Extracts arguments from lists or tuples and returns them
#     "flattened" (extracting lists within lists to a flat list).
#
#     Args:
#         args: can be any argument.
#
#     Returns:
#         list
#     """
#     flat = []
#     for arg in args:
#         if isinstance(arg, (list, tuple, pd.Index)):
#             flat.extend(_arg_extractor(arg))
#         else:
#             flat.append(arg)
#     return flat


# def flatten_arguments(f):
#     """Decorator that "flattens" any arguments contained inside of lists or
#     tuples. Designed primarily for selection and dropping functions.
#
#     Example:
#         args = (a, b, (c, d, [e, f, g]))
#         becomes
#         args = (a, b, c, d, e, f, g)
#
#     Args:
#         f (function): function for which the arguments should be flattened.
#
#     Returns:
#         decorated function
#     """
#     @wraps(f)
#     def wrapped(*args, **kwargs):
#         assert len(args) > 0 and isinstance(args[0], pd.DataFrame)
#         if len(args) > 1:
#             flat_args = [args[0]]+_arg_extractor(args[1:])
#             return f(*flat_args, **kwargs)
#         else:
#             return f(*args, **kwargs)
#     return wrapped
#
#
# def join_index_arguments(f):
#     """Decorator for joining indexing arguments together. Designed primarily for
#     `row_slice` to combine arbitrary single indices and lists of indices
#     together.
#
#     Example:
#         args = (1, 2, 3, [4, 5], [6, 7])
#         becomes
#         args = ([1, 2, 3, 4, 5, 6, 7])
#
#     Args:
#         f (function): function to be decorated.
#
#     Returns:
#         decorated function
#     """
#     @wraps(f)
#     def wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         if len(args) > 1:
#             args_ = reduce(lambda x, y: np.concatenate([np.atleast_1d(x), np.atleast_1d(y)]),
#                            args[1:])
#             args = [args[0]] + [np.atleast_1d(args_)]
#         return f(*args, **kwargs)
#     return wrapped
#
#
#
# def _col_ind_to_position(columns, indexer):
#     """Converts column indexers to their integer position.
#
#     Args:
#         columns (list): list of column names.
#         indexer (str or int): either a column name or an integer position of the
#             column.
#
#     Returns:
#         Integer column position.
#     """
#     if isinstance(indexer, str):
#         if indexer not in columns:
#             raise Exception("String label "+str(indexer)+' is not in columns.')
#         return columns.index(indexer)
#     elif isinstance(indexer, int):
#         #if indexer < 0:
#         #    raise Exception("Int label "+str(indexer)+' is negative. Not currently allowed.')
#         return indexer
#     else:
#         raise Exception("Column indexer not of type str or int.")
#
#
#
# def _col_ind_to_label(columns, indexer):
#     """Converts column indexers positions to their string label.
#
#     Args:
#         columns (list): list of column names.
#         indexer (int or str): either a column name or an integer position of
#             the column.
#
#     Returns:
#         String column name.
#     """
#     if isinstance(indexer, str):
#         return indexer
#     elif isinstance(indexer, int):
#         warnings.warn('Int labels will be inferred as column positions.')
#         if indexer < -1*len(columns):
#             raise Exception(str(indexer)+' is negative and less than length of columns.')
#         elif indexer >= len(columns):
#             raise Exception(str(indexer)+' is greater than length of columns.')
#         else:
#             return list(columns)[indexer]
#     else:
#         raise Exception("Label not of type str or int.")
#
#
# def column_indices_as_labels(f):
#     """Decorator that convertes column indicies to label. Typically decoration
#     occurs after decoration by `symbolic_reference`.
#
#     Args:
#         f (function): function to be decorated.
#
#     Returns:
#         Decorated function with any column indexers converted to their string
#         labels.
#     """
#     @wraps(f)
#     def wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         if len(args) > 1:
#             label_args = [_col_ind_to_label(args[0].columns.tolist(), arg)
#                           for arg in args[1:]]
#             args = [args[0]]+label_args
#         return f(*args, **kwargs)
#     return wrapped
#
#
# def column_indices_as_positions(f):
#     """Decorator that converts column indices to integer position. Typically
#     decoration occurs after decoration by `symbolic_reference`.
#
#     Args:
#         f (function): function to be decorated.
#
#     Returns:
#         Decorated function with any column indexers converted to their integer
#         positions.
#     """
#     @wraps(f)
#     def wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         if len(args) > 1:
#             label_args = [_col_ind_to_position(args[0].columns.tolist(), arg)
#                           for arg in args[1:]]
#             args = [args[0]]+label_args
#         return f(*args, **kwargs)
#     return wrapped
#
#
#
# def label_selection(f):
#     """Convenience chain of decorators for functions that operate with the
#     expectation of having column labels as arguments (despite user potentially
#     providing symbolic `pandas.Series` objects or integer column positions).
#
#     Args:
#         f (function): function to be decorated.
#
#     Returns:
#         Decorated function with any column indexers converted to their string
#         labels and arguments flattened.
#     """
#     return pipe(
#         symbolic_reference(
#             flatten_arguments(
#                 column_indices_as_labels(f)
#             )
#         )
#     )
#
#
# def positional_selection(f):
#     """Convenience chain of decorators for functions that operate with the
#     expectation of having column integer positions as arguments (despite
#     user potentially providing symbolic `pandas.Series` objects or column labels).
#
#     Args:
#         f (function): function to be decorated.
#
#     Returns:
#         Decorated function with any column indexers converted to their integer
#         positions and arguments flattened.
#     """
#     return pipe(
#         symbolic_reference(
#             flatten_arguments(
#                 column_indices_as_positions(f)
#             )
#         )
#     )
#
#
#
# def dfpipe(f):
#     """Standard chain of decorators for a function to be used with dfply.
#     The function can be chained with >> by `pipe`, application of the function
#     to grouped DataFrames is enabled by `group_delegation`, and symbolic
#     arguments are evaluated as-is using a default `symbolic_evaluation`.
#
#     Args:
#         f (function): function to be decorated.
#
#     Returns:
#         Decorated function chaining the `pipe`, `group_delegation`, and
#         `symbolic_evaluation` decorators.
#     """
#     return pipe(
#         group_delegation(
#             symbolic_evaluation(f)
#         )
#     )
