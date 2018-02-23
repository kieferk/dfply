import pandas as pd
import numpy as np
import warnings
from functools import partial, wraps


def _recursive_apply(f, l):
    if isinstance(l, (list, tuple)):
        out = [_recursive_apply(f, l_) for l_ in l]
        if isinstance(l, tuple):
            out = tuple(out)
        return out
    else:
        return f(l)


def contextualize(arg, context):
    if isinstance(arg, Intention):
        arg = arg.evaluate(context)
    return arg


def flatten(l):
    for el in l:
        if isinstance(el, (tuple, list)):
            yield from flatten(el)
        else:
            yield el


def _check_delayed_eval(args, kwargs):
    check = lambda x: isinstance(x, Intention)
    delay = any([a for a in flatten(_recursive_apply(check, args))])
    delay = delay or any([v for v in flatten(_recursive_apply(check, list(kwargs.values())))])
    return delay


def _context_args(args):
    return lambda x: _recursive_apply(partial(contextualize, context=x), args)


def _context_kwargs(kwargs):
    values_ = lambda x: _recursive_apply(partial(contextualize, context=x),
                                         list(kwargs.values()))
    return lambda x: {k: v for k, v in zip(kwargs.keys(), values_(x))}


def _delayed_function(function, args, kwargs):
    return lambda x: function(*_context_args(args)(x),
                              **_context_kwargs(kwargs)(x))


def make_symbolic(f):
    """Decorator to convert a function into a symbolic function. Decorating a
    function with `make_symbolic` enables the use of symbolic arguments that
    will need to be delayed later.

    Example:
        Sometimes, like in the window and summary functions that operate on
        series, it is necessary to defer the evaluation of a function. For
        example, in the code below:

        >>> diamonds >> summarize(price_third=nth(X.price, 3))

        The `nth()`` function would typically be evaluated before `summarize()`
        and the symbolic argument `X.price` would not be evaluated at the right
        time.

        The `@make_symbolic` decorator can be placed above functions to convert
        them into symbolic functions that will wait to evaluate. Again, this is
        used primarily for functions that are embedded inside the function call
        within the piping syntax.

        The nth() code, for example, is below:

        ```python
        @make_symbolic
        def nth(series, n, order_by=None):
            if order_by is not None:
                series = order_series_by(series, order_by)
            try:
                return series.iloc[n]
            except:
                return np.nan
        ```

        Functions you write that you want to be able to embed as an argument can
        use the @make_symbolic to wait until they have access to the DataFrame
        to evaluate.

    """
    def wrapper(*args, **kwargs):
        delay = _check_delayed_eval(args, kwargs)
        if delay:
            delayed = _delayed_function(f, args, kwargs)
            return Intention(delayed)
        else:
            return f(*args, **kwargs)

    return wrapper


class Intention(object):
     """The `Intention` class defers evaluation. Python does not evaluate lazily
     by design; this is emulated by chaining lambda functions.

    `Intention` is not intended to be used directly. As the name implies it is
    the representation of an intended action to be done at a later time.
    """
    def __init__(self, function=lambda x: x, invert=False):
        self.function = function
        self.inverted = invert

    def evaluate(self, context):
        return self.function(context)

    def __getattr__(self, attribute):
        return Intention(lambda x: getattr(self.function(x), attribute),
                         invert=self.inverted)

    def __invert__(self):
        return Intention(self.function, invert=not self.inverted)

    def __call__(self, *args, **kwargs):
        return Intention(lambda x: self.function(x)(*_context_args(args)(x),
                                                    **_context_kwargs(kwargs)(x)),
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
    '__rand__', '__rcmp__', '__rdiv__', '__rdivmod__',  # '__repr__',
    '__reversed__', '__rfloordiv__', '__rlshift__', '__rmod__', '__rmul__',
    '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__',
    '__rtruediv__', '__rxor__', '__set__', '__setitem__', '__setslice__',
    '__sub__', '__truediv__', '__unicode__', '__xor__', '__str__',
]


def _set_magic_method(name):
    def magic_method(self, *args, **kwargs):
        return Intention(lambda x: getattr(self.function(x), name)(*_context_args(args)(x),
                                                                   **_context_kwargs(kwargs)(x)),
                         invert=self.inverted)

    return magic_method


for name in _magic_method_names:
    setattr(Intention, name, _set_magic_method(name))

# Initialize the global X symbol
X = Intention()


class pipe(object):
    """The pipe decorator allows functions to send a dataframe from one operation
    to the next via the >> piping operator.

    For functions to work with the piping syntax they must be decorated with `@pipe.`

    Any function decorated with `@pipe` implicitly receives a single first
    argument expected to be a pandas DataFrame. This is the DataFrame being
    passed through the pipe. For example, mutate and select have function
    specifications mutate(df, **kwargs) and select(df, *args, **kwargs),
    but when used do not require the user to insert the DataFrame as an argument.

    ```python
    diamonds >> mutate(new_var=X.price + X.depth) >> select(X.new_var)
    ```

    If you create a new function decorated by `@pipe`, the function definition
    should contain an initial argument that represents the DataFrame being
    passed through the piping operations.

    ```python
    @pipe
    def myfunc(df, *args, **kwargs):
      # code
    ```
    """

    __name__ = "pipe"

    def __init__(self, function):
        self.function = function
        self.__doc__ = function.__doc__

        self.chained_pipes = []

    def __rshift__(self, other):
        assert isinstance(other, pipe)
        self.chained_pipes.append(other)
        return self

    def __rrshift__(self, other):
        other_copy = other.copy()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            other_copy._grouped_by = getattr(other, '_grouped_by', None)

        result = self.function(other_copy)

        for p in self.chained_pipes:
            result = p.__rrshift__(result)
        return result

    def __call__(self, *args, **kwargs):
        return pipe(lambda x: self.function(x, *args, **kwargs))


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
        self.__doc__ = function.__doc__

        self.eval_symbols = eval_symbols
        self.eval_as_label = eval_as_label
        self.eval_as_selector = eval_as_selector

    def _evaluate(self, df, arg):
        if isinstance(arg, Intention):
            negate = arg.inverted
            arg = arg.evaluate(df)
            if negate:
                arg = ~arg
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
            for i, a in enumerate(args)
        ]

    def _recursive_kwarg_eval(self, df, kwargs):
        eval_symbols = self._find_eval_kwargs(self.eval_symbols, kwargs)
        eval_as_label = self._find_eval_kwargs(self.eval_as_label, kwargs)
        eval_as_selector = self._find_eval_kwargs(self.eval_as_selector, kwargs)

        return {
            k: (self._symbolic_to_label(df, v) if k in eval_as_label
                else self._symbolic_to_selector(df, v) if k in eval_as_selector
            else self._symbolic_eval(df, v) if k in eval_symbols
            else v)
            for k, v in kwargs.items()
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
        @wraps(function)
        def wrapper(function):
            return IntentionEvaluator(function, eval_symbols=eval_symbols,
                                      eval_as_label=eval_as_label,
                                      eval_as_selector=eval_as_selector)

        return wrapper


class group_delegation(object):
    __name__ = "group_delegation"

    def __init__(self, function):
        self.function = function
        self.__doc__ = function.__doc__

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

    def __call__(self, *args, **kwargs):
        grouped_by = getattr(args[0], '_grouped_by', None)
        if (grouped_by is None) or not all([g in args[0].columns for g in grouped_by]):
            return self.function(*args, **kwargs)
        else:
            applied = self._apply(args[0], *args[1:], **kwargs)

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                applied._grouped_by = grouped_by

            return applied


class degroup(object):
    __name__ = "degroup"

    def __init__(self, function):
        self.function = function
        self.__doc__ = function.__doc__


    def __call__(self, *args, **kwargs):
        applied = self.function(*args, **kwargs)
        setattr(applied, '_grouped_by', None)
        return applied



def dfpipe(f):
    return pipe(
        group_delegation(
            symbolic_evaluation(f)
        )
    )


def dfpipe_degroup(f):
    return pipe(
        degroup(
            group_delegation(
                symbolic_evaluation(f)
                )
            )
        )
