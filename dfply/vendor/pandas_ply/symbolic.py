"""`ply.symbolic` is a simple system for building "symbolic expressions" to
provide as arguments to **pandas-ply**'s methods (in place of lambda
expressions)."""

from ..six import print_
from ..six import iteritems


class Expression(object):
    """`Expression` is the (abstract) base class for symbolic expressions.
    Symbolic expressions are encoded representations of Python expressions,
    kept on ice until you are ready to evaluate them. Operations on
    symbolic expressions (like `my_expr.some_attr` or `my_expr(some_arg)` or
    `my_expr + 7`) are automatically turned into symbolic representations
    thereof -- nothing is actually done until the special evaluation method
    `_eval` is called.
    """

    def _eval(self, context, **options):
        """Evaluate a symbolic expression.

        Args:
            context: The context object for evaluation. Currently, this is a
                dictionary mapping symbol names to values,
            `**options`: Options for evaluation. Currently, the only option is
                `log`, which results in some debug output during evaluation if
                it is set to `True`.

        Returns:
            anything
        """
        raise NotImplementedError

    def __repr__(self):
        raise NotImplementedError

    def __getattr__(self, name):
        """Construct a symbolic representation of `getattr(self, name)`."""
        return GetAttr(self, name)

    def __call__(self, *args, **kwargs):
        """Construct a symbolic representation of `self(*args, **kwargs)`."""
        return Call(self, args=args, kwargs=kwargs)

# New-style classes skip __getattr__ for magic methods, so we must add them
# explicitly:

_magic_method_names = [
    '__abs__', '__add__', '__and__', '__cmp__', '__complex__', '__contains__',
    '__delattr__', '__delete__', '__delitem__', '__delslice__', '__div__',
    '__divmod__', '__enter__', '__eq__', '__exit__', '__float__',
    '__floordiv__', '__ge__', '__get__', '__getitem__', '__getslice__',
    '__gt__', '__hash__', '__hex__', '__iadd__', '__iand__', '__idiv__',
    '__ifloordiv__', '__ilshift__', '__imod__', '__imul__', '__index__',
    '__int__', '__invert__', '__ior__', '__ipow__', '__irshift__', '__isub__',
    '__iter__', '__itruediv__', '__ixor__', '__le__', '__len__', '__long__',
    '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__', '__neg__',
    '__nonzero__', '__oct__', '__or__', '__pos__', '__pow__', '__radd__',
    '__rand__', '__rcmp__', '__rdiv__', '__rdivmod__', '__repr__',
    '__reversed__', '__rfloordiv__', '__rlshift__', '__rmod__', '__rmul__',
    '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__',
    '__rtruediv__', '__rxor__', '__set__', '__setitem__', '__setslice__',
    '__str__', '__sub__', '__truediv__', '__unicode__', '__xor__',
]

# Not included: [
#   '__call__', '__coerce__', '__del__', '__dict__', '__getattr__',
#   '__getattribute__', '__init__', '__new__', '__setattr__'
# ]

def _get_sym_magic_method(name):
    def magic_method(self, *args, **kwargs):
        return Call(GetAttr(self, name), args, kwargs)
    return magic_method

for name in _magic_method_names:
    setattr(Expression, name, _get_sym_magic_method(name))


# Here are the varieties of atomic / compound Expression.


class Symbol(Expression):
    """`Symbol(name)` is an atomic symbolic expression, labelled with an
    arbitrary `name`."""

    def __init__(self, name):
        self._name = name

    def _eval(self, context, **options):
        if options.get('log'):
            print_('Symbol._eval', repr(self))
        result = context[self._name]
        if options.get('log'):
            print_('Returning', repr(self), '=>', repr(result))
        return result

    def __repr__(self):
        return 'Symbol(%s)' % repr(self._name)


class GetAttr(Expression):
    """`GetAttr(obj, name)` is a symbolic expression representing the result of
    `getattr(obj, name)`. (`obj` and `name` can themselves be symbolic.)"""

    def __init__(self, obj, name):
        self._obj = obj
        self._name = name

    def _eval(self, context, **options):
        if options.get('log'):
            print_('GetAttr._eval', repr(self))
        evaled_obj = eval_if_symbolic(self._obj, context, **options)
        result = getattr(evaled_obj, self._name)
        if options.get('log'):
            print_('Returning', repr(self), '=>', repr(result))
        return result

    def __repr__(self):
        return 'getattr(%s, %s)' % (repr(self._obj), repr(self._name))


class Call(Expression):
    """`Call(func, args, kwargs)` is a symbolic expression representing the
    result of `func(*args, **kwargs)`. (`func`, each member of the `args`
    iterable, and each value in the `kwargs` dictionary can themselves be
    symbolic)."""

    def __init__(self, func, args=[], kwargs={}):
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def _eval(self, context, **options):
        if options.get('log'):
            print_('Call._eval', repr(self))
        evaled_func = eval_if_symbolic(self._func, context, **options)
        evaled_args = [eval_if_symbolic(v, context, **options)
                       for v in self._args]
        evaled_kwargs = dict((k, eval_if_symbolic(v, context, **options))
                             for k, v in iteritems(self._kwargs))
        result = evaled_func(*evaled_args, **evaled_kwargs)
        if options.get('log'):
            print_('Returning', repr(self), '=>', repr(result))
        return result

    def __repr__(self):
        return '{func}(*{args}, **{kwargs})'.format(
            func=repr(self._func),
            args=repr(self._args),
            kwargs=repr(self._kwargs))


def eval_if_symbolic(obj, context, **options):
    """Evaluate an object if it is a symbolic expression, or otherwise just
    returns it back.

    Args:
        obj: Either a symbolic expression, or anything else (in which case this
            is a noop).
        context: Passed as an argument to `obj._eval` if `obj` is symbolic.
        `**options`: Passed as arguments to `obj._eval` if `obj` is symbolic.

    Returns:
        anything

    Examples:
        >>> eval_if_symbolic(Symbol('x'), {'x': 10})
        10
        >>> eval_if_symbolic(7, {'x': 10})
        7
    """
    return obj._eval(context, **options) if hasattr(obj, '_eval') else obj


def to_callable(obj):
    """Turn an object into a callable.

    Args:
        obj: This can be

            * **a symbolic expression**, in which case the output callable
              evaluates the expression with symbols taking values from the
              callable's arguments (listed arguments named according to their
              numerical index, keyword arguments named according to their
              string keys),
            * **a callable**, in which case the output callable is just the
              input object, or
            * **anything else**, in which case the output callable is a
              constant function which always returns the input object.

    Returns:
        callable

    Examples:
        >>> to_callable(Symbol(0) + Symbol('x'))(3, x=4)
        7
        >>> to_callable(lambda x: x + 1)(10)
        11
        >>> to_callable(12)(3, x=4)
        12
    """
    if hasattr(obj, '_eval'):
        return lambda *args, **kwargs: obj._eval(dict(enumerate(args), **kwargs))
    elif callable(obj):
        return obj
    else:
        return lambda *args, **kwargs: obj


def sym_call(func, *args, **kwargs):
    """Construct a symbolic representation of `func(*args, **kwargs)`.

    This is necessary because `func(symbolic)` will not (ordinarily) know to
    construct a symbolic expression when it receives the symbolic
    expression `symbolic` as a parameter (if `func` is not itself symbolic).
    So instead, we write `sym_call(func, symbolic)`.

    Tip: If the main argument of the function is a (symbolic) DataFrame, then
    pandas' `pipe` method takes care of this problem without `sym_call`. For
    instance, while `np.sqrt(X)` won't work, `X.pipe(np.sqrt)` will.

    Args:
        func: Function to call on evaluation (can be symbolic).
        `*args`: Arguments to provide to `func` on evaluation (can be symbolic).
        `**kwargs`: Keyword arguments to provide to `func` on evaluation (can be
            symbolic).

    Returns:
        `ply.symbolic.Expression`

    Example:
        >>> sym_call(math.sqrt, Symbol('x'))._eval({'x': 16})
        4
    """

    return Call(func, args=args, kwargs=kwargs)

X = Symbol(0)
"""A Symbol for "the first argument" (for convenience)."""
