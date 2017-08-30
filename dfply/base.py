import pandas as pd
import numpy as np
import warnings
from functools import partial


#
# sensitivity = TP / (TP + FN)
# specificity = TN / (TN + FP)

# TN + TP + FN + FP = N

# condition positive = (TP + FN) = N - TN - FP
# condition negative = (TN + FP) = N - TP - FN

# sen = TP / (N - TN - FP)
# sen = (N - TN - FN - FP) / (N - TN - FP)


# sen^2 = TP^2 / (TP + FN)^2
# spe^2 = TN^2 / (TN + FP)^2
#
#
# d = (1-(TN / (TN + FP)))**2 + (1 - (TP / (TP + FN)))**2
#
#


def contextualize(arg, context):
    if isinstance(arg, Intention):
        arg = arg.evaluate(context)
    return arg


class Intention(object):

    def __init__(self, function=lambda x: x, invert=False):
        self.function = function
        self.inverted = invert

    def evaluate(self, context):
        return self.function(context)

    def __getattr__(self, attribute):
        return Intention(lambda x: getattr(self.function(x), attribute), invert=self.inverted)

    def __invert__(self):
        return Intention(self.function, invert=not self.inverted)

    def __call__(self, *args, **kwargs):
        return Intention(lambda x: self.function(x)(*(contextualize(a, x) for a in args),
                                                    **{k:contextualize(v, x) for k,v in kwargs.items()}),
                         invert=self.inverted)



_magic_method_names = [
    '__abs__', '__add__', '__and__', '__cmp__', '__complex__', '__contains__',
    '__delattr__', '__delete__', '__delitem__', '__delslice__', '__div__',
    '__divmod__', '__enter__', '__eq__', '__exit__', '__float__',
    '__floordiv__', '__ge__', '__get__', '__getitem__', '__getslice__',
    '__gt__', '__hash__', '__hex__', '__iadd__', '__iand__', '__idiv__',
    '__ifloordiv__', '__ilshift__', '__imod__', '__imul__', '__index__',
    '__int__', '__ior__', '__ipow__', '__irshift__', '__isub__',
    '__iter__', '__itruediv__', '__ixor__', '__le__', '__len__', '__long__',
    '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__', '__neg__',
    '__nonzero__', '__oct__', '__or__', '__pos__', '__pow__', '__radd__',
    '__rand__', '__rcmp__', '__rdiv__', '__rdivmod__', #'__repr__',
    '__reversed__', '__rfloordiv__', '__rlshift__', '__rmod__', '__rmul__',
    '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__',
    '__rtruediv__', '__rxor__', '__set__', '__setitem__', '__setslice__',
    '__sub__', '__truediv__', '__unicode__', '__xor__', '__str__',
]

def _set_magic_method(name):
    def magic_method(self, *args, **kwargs):
        return Intention(lambda x: getattr(self.function(x), name)(*(contextualize(a, x) for a in args),
                                                                   **{k:contextualize(v, x) for k,v in kwargs.items()}),
                         invert=self.inverted)
    return magic_method

for name in _magic_method_names:
    setattr(Intention, name, _set_magic_method(name))


# Initialize the global X symbol
X = Intention()


class pipe(object):

    __name__ = "pipe"

    def __init__(self, function):
        self.function = function
        self.chained_pipes = []


    def __rshift__(self, other):
        assert isinstance(other, pipe)
        self.chained_pipes.append(other)
        return self


    def __rrshift__(self, other):
        other_copy = other.copy()
        other_copy._grouped_by = getattr(other, '_grouped_by', None)

        result = self.function(other_copy)

        #r_grouped_by = getattr(result, '_grouped_by', None)
        #if r_grouped_by is None:
        #    result._grouped_by = getattr(other, '_grouped_by', None)

        #print(result.head(3))
        #print(getattr(result, '_grouped_by', None))
        #if not grouped_by is None:
            #print(grouped_by)
            #result._grouped_by = grouped_by

        for p in self.chained_pipes:
            result = p.__rrshift__(result)
        return result


    def __call__(self, *args, **kwargs):
        return pipe(lambda x: self.function(x, *args, **kwargs))




def flatten(l):
    for el in l:
        if isinstance(el, (tuple, list)):
            yield from flatten(el)
        else:
            yield el


class IntentionEvaluator(object):
    """
    Parent class for symbolic argument decorators.
    Default behavior is to recursively turn the arguments and keyword
    arguments of a decorated function into `symbolic.Call` objects that
    can be evaluated against a pandas DataFrame as it comes down a pipe.
    """

    __name__ = "IntentionEvaluator"

    def __init__(self, function, eval_symbols=True, eval_as_label=[],
                 eval_as_selector=[]):
        super(IntentionEvaluator, self).__init__()
        self.function = function

        self.eval_symbols = eval_symbols
        self.eval_as_label = eval_as_label
        self.eval_as_selector = eval_as_selector


    def _evaluate(self, df, arg):
        if isinstance(arg, Intention):
            return arg.evaluate(df)
        else:
            return arg

    def _evaluate_label(self, df, arg):
        arg = self._evaluate(df, arg)

        cols = list(df.columns)
        if isinstance(arg, pd.Series):
            arg = arg.name
        if isinstance(arg, pd.Index):
            arg = list(arg)
        if isinstance(arg, int):
            arg = cols[arg]
        return arg


    def _evaluate_selector(self, df, arg):
        negate = False
        if isinstance(arg, Intention):
            negate = arg.inverted
            arg = arg.evaluate(df)

        cols = list(df.columns)
        if isinstance(arg, pd.Series):
            arg = [cols.index(arg.name)]
        if isinstance(arg, pd.Index):
            arg = [cols.index(i) for i in list(arg)]
        if isinstance(arg, pd.DataFrame):
            arg = [cols.index(i) for i in arg.columns]
        if isinstance(arg, int):
            arg = [arg]
        if isinstance(arg, str):
            arg = [cols.index(arg)]
        if isinstance(arg, (list, tuple)):
            arg = [cols.index(i) if isinstance(i, str) else i for i in arg]

        selection_vector = np.zeros(df.shape[1])
        col_idx = np.array(arg)

        if negate and len(col_idx) > 0:
            selection_vector[col_idx] = -1
        elif len(col_idx) > 0:
            selection_vector[col_idx] = 1
        return selection_vector


    def _evaluator_loop(self, df, arg, eval_func):
        if isinstance(arg, (list, tuple)):
            return [self._evaluator_loop(df, a_, eval_func) for a_ in arg]
        else:
            return eval_func(df, arg)

    def _symbolic_eval(self, df, arg):
        return self._evaluator_loop(df, arg, self._evaluate)


    def _symbolic_to_label(self, df, arg):
        return self._evaluator_loop(df, arg, self._evaluate_label)


    def _symbolic_to_selector(self, df, arg):
        return self._evaluator_loop(df, arg, self._evaluate_selector)


    def _recursive_arg_eval(self, df, args):
        eval_symbols = self._find_eval_args(self.eval_symbols, args)
        eval_as_label = self._find_eval_args(self.eval_as_label, args)
        eval_as_selector = self._find_eval_args(self.eval_as_selector, args)

        return [
            self._symbolic_to_label(df, a) if i in eval_as_label
            else self._symbolic_to_selector(df, a) if i in eval_as_selector
            else self._symbolic_eval(df, a) if i in eval_symbols
            else a
            for i,a in enumerate(args)
                ]


    def _recursive_kwarg_eval(self, df, kwargs):
        eval_symbols = self._find_eval_kwargs(self.eval_symbols, kwargs)
        eval_as_label = self._find_eval_kwargs(self.eval_as_label, kwargs)
        eval_as_selector = self._find_eval_kwargs(self.eval_as_selector, kwargs)

        return {
            k:(self._symbolic_to_label(df, v) if k in eval_as_label
               else self._symbolic_to_selector(df, v) if k in eval_as_selector
               else self._symbolic_eval(df, v) if k in eval_symbols
               else v)
            for k,v in kwargs.items()
            }


    def _find_eval_args(self, request, args):
        if (request == True) or ('*' in request):
            return [i for i in range(len(args))]
        elif request in [None, False]:
            return []
        return request


    def _find_eval_kwargs(self, request, kwargs):
        if (request == True) or ('**' in request):
            return [k for k in kwargs.keys()]
        elif request in [None, False]:
            return []
        return request


    def __call__(self, *args, **kwargs):
        df = args[0]

        args = self._recursive_arg_eval(df, args[1:])
        kwargs = self._recursive_kwarg_eval(df, kwargs)

        return self.function(df, *args, **kwargs)



def symbolic_evaluation(function=None, eval_symbols=True, eval_as_label=[],
                        eval_as_selector=[]):
    if function:
        return IntentionEvaluator(function)
    else:
        def wrapper(function):
            return IntentionEvaluator(function, eval_symbols=eval_symbols,
                                      eval_as_label=eval_as_label,
                                      eval_as_selector=eval_as_selector)
        return wrapper



# class _as_selection_vector(symbolic_evaluation):
#
#     def _evaluate(self, df, arg):
#         if isinstance(arg, Intention):
#             negate = arg.inverted
#             evaled = arg.evaluate(df)
#         else:
#             negate = False
#             evaled = arg
#
#         cols = list(df.columns)
#         if isinstance(evaled, pd.Series):
#             evaled = [cols.index(evaled.name)]
#         if isinstance(evaled, pd.Index):
#             evaled = [cols.index(i) for i in list(evaled)]
#         if isinstance(evaled, pd.DataFrame):
#             evaled = [cols.index(i) for i in evaled.columns]
#         if isinstance(evaled, int):
#             evaled = [evaled]
#         if isinstance(evaled, str):
#             evaled = [cols.index(evaled)]
#         if isinstance(evaled, (tuple, list)):
#             evaled = [cols.index(e) if isinstance(e, str) else e for e in evaled]
#         col_idx = np.array(evaled)
#         selection_vector = np.zeros(df.shape[1])
#         if negate:
#             selection_vector[col_idx] = -1
#         else:
#             selection_vector[col_idx] = 1
#         return selection_vector
#
#
# class _as_column_labels(symbolic_evaluation):
#
#     def _evaluate(self, df, arg):
#         if isinstance(arg, Intention):
#             evaled = arg.evaluate(df)
#         else:
#             evaled = arg
#
#         #print(evaled)
#
#         cols = list(df.columns)
#         if isinstance(evaled, pd.Series):
#             evaled = evaled.name
#         if isinstance(evaled, pd.Index):
#             evaled = list(evaled)
#         if isinstance(evaled, int):
#             evaled = cols[evaled]
#         if isinstance(evaled, (tuple, list)):
#             evaled = [cols[e] if isinstance(e, int) else e for e in evaled]
#         return evaled
#
#
# def as_selection_vector(function=None, eval_args=True, eval_kwargs=True):
#     if function:
#         return _as_selection_vector(function)
#     else:
#         def wrapper(function):
#             return _as_selection_vector(function, eval_args=eval_args,
#                                         eval_kwargs=eval_kwargs)
#         return wrapper
#
#
# def as_column_labels(function=None, eval_args=True, eval_kwargs=False):
#     if function:
#         return _as_column_labels(function)
#     else:
#         def wrapper(function):
#             return _as_column_labels(function, eval_args=eval_args,
#                                      eval_kwargs=eval_kwargs)
#         return wrapper




class group_delegation(object):

    __name__ = "group_delegation"

    def __init__(self, function):
        self.function = function


    def _apply(self, df, *args, **kwargs):
        grouped = df.groupby(df._grouped_by)

        df = grouped.apply(self.function, *args, **kwargs)

        for name in df.index.names[:-1]:
            if name in df:
                df.reset_index(level=0, drop=True, inplace=True)
            else:
                df.reset_index(level=0, inplace=True)

        if (df.index == 0).all():
            df.reset_index(drop=True, inplace=True)

        return df
        #return df.sort_index()


    def __call__(self, *args, **kwargs):
        grouped_by = getattr(args[0], '_grouped_by', None)
        #print(grouped_by)
        if (grouped_by is None) or not all([g in args[0].columns for g in grouped_by]):
            #print('or here', args[1:])
            return self.function(*args, **kwargs)
        else:
            #print('here', args[1:])
            applied = self._apply(args[0], *args[1:], **kwargs)
            applied._grouped_by = grouped_by
            return applied



def dfpipe(f):
    return pipe(
        group_delegation(
            symbolic_evaluation(f)
        )
    )



def recursive_apply(f, l):
    if isinstance(l, (tuple, list)):
        return [recursive_apply(f, l_) for l_ in l]
    else:
        return f(l)


def make_symbolic(f):
    def wrapper(*args, **kwargs):
        #delay = any(map(lambda a: isinstance(a, Intention), args))
        #delay = delay or any(map(lambda v: isinstance(v, Intention), kwargs.values()))
        check = lambda x: isinstance(x, Intention)
        delay = any([a for a in flatten(recursive_apply(check, args))])
        delay = delay or any([v for v in flatten(recursive_apply(check, list(kwargs.values())))])
        if delay:
            args_ = lambda x: recursive_apply(partial(contextualize, context=x), args)
            kwargs_v_ = lambda x: recursive_apply(partial(contextualize, context=x), list(kwargs.values()))
            return Intention(lambda x: f(*args_(x),
                                         **{k:v for k,v in zip(kwargs.keys(), kwargs_v_(x))}))

            #return Intention(lambda x: f(*(contextualize(a, x) for a in args),
            #                             **{k:contextualize(v, x) for k,v in kwargs.items()}))
        else:
            return f(*args, **kwargs)
    return wrapper






#
#
# class group_delegation(object):
#     """Decorator class that managing grouped operations on DataFrames.
#
#     Checks for an attached `df._grouped_by` attribute added to a
#     pandas DataFrame by the `groupby` function.
#
#     If groups are found, the operation defined by the function is
#     carried out for each group individually. The internal
#     `_apply_combine_reset` function ensures that hierarchical
#     indexing is removed.
#     """
#
#     __name__ = "group_delegation"
#
#     def __init__(self, function):
#         self.function = function
#
#
#     def _apply_combine_reset(self, grouped, *args, **kwargs):
#         combined = grouped.apply(self.function, *args, **kwargs)
#
#         for name in combined.index.names[:-1]:
#             if name in combined:
#                 combined.reset_index(level=0, drop=True, inplace=True)
#             else:
#                 combined.reset_index(level=0, inplace=True)
#
#         if (combined.index == 0).all():
#             combined.reset_index(drop=True, inplace=True)
#
#         return combined
#
#
#     def __call__(self, *args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#
#         df = args[0]
#         grouped_by = getattr(df, "_grouped_by", None)
#
#         if grouped_by is not None:
#             df = df.groupby(grouped_by)
#
#             try:
#                 assert self.function.function.__name__ == 'transmute'
#                 pass_args = grouped_by
#             except:
#                 pass_args = args[1:]
#
#             df = self._apply_combine_reset(df, *pass_args, **kwargs)
#             if all([True if group in df.columns else False for group in grouped_by]):
#                 df._grouped_by = grouped_by
#             else:
#                 warnings.warn('Grouping lost during transformation.')
#             return df
#
#         else:
#             return self.function(*args, **kwargs)
#
#
#
# class SymbolicHandler(object):
#     """
#     Parent class for symbolic argument decorators.
#
#     Default behavior is to recursively turn the arguments and keyword
#     arguments of a decorated function into `symbolic.Call` objects that
#     can be evaluated against a pandas DataFrame as it comes down a pipe.
#     """
#
#     __name__ = "SymbolicHandler"
#     call_has_symbolic = False
#     df = None
#
#
#     def __init__(self, function):
#         super(SymbolicHandler, self).__init__()
#         self.function = function
#
#
#     def argument_symbolic_eval(self, arg):
#         if isinstance(arg, (list, tuple)):
#             arglist = [self.argument_symbolic_eval(subarg) for subarg in arg]
#             return symbolic.sym_call(lambda *x: x, *arglist)
#         else:
#             if isinstance(arg, symbolic.Expression):
#                 self.call_has_symbolic = True
#             return arg
#
#
#     def argument_symbolic_reference(self, arg):
#         if hasattr(arg, '_eval'):
#             arg = symbolic.to_callable(arg)(self.df)
#         if isinstance(arg, pd.Series):
#             return arg.name
#         elif isinstance(arg, pd.DataFrame):
#             return symbolic.sym_call(lambda *x: x, arg.columns.tolist())
#         elif isinstance(arg, (list, tuple)):
#             arglist = [self.argument_symbolic_reference(subarg) for subarg in arg]
#             return symbolic.sym_call(lambda *x: x, *arglist)
#         return arg
#
#
#     def recurse_args(self, args):
#         return [self.arg_action(arg) for arg in args]
#
#
#     def recurse_kwargs(self, kwargs):
#         return {k:self.kwarg_action(v) for k,v in kwargs.items()}
#
#
#     def arg_action(self, arg):
#         raise NotImplementedError("Subclass must implement actions for args.")
#
#
#     def kwarg_action(self, kwarg):
#         raise NotImplementedError("Subclass must implement actions for kwargs.")
#
#
#     def call_action(self, args, kwargs):
#         raise NotImplementedError("Subclass must implement action for call.")
#
#
#     def __call__(self, *args, **kwargs):
#         evaluation = self.call_action(args, kwargs)
#         self.call_has_symbolic = False
#         return evaluation
#
#
#
# class make_symbolic(SymbolicHandler):
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
#     __name__ = "make_symbolic"
#
#
#     def __init__(self, function):
#         super(make_symbolic, self).__init__(function)
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
#
#         if not self.call_has_symbolic:
#             return symbolic.eval_if_symbolic(symbolic_function, {})
#         else:
#             return symbolic_function
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
#
#
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
