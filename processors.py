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

def _default_fn(x):
    return x

def _get_series_name(s):
    return s.name

def _get_column_names(df):
    return df.columns.tolist()

def _eval_against_df(x, df=None):
    return symbolic.to_callable(x)(df)

def _eval_without_context(x):
    return symbolic.eval_if_symbolic(x, {})

def _list_to_symbolic(args):
    return symbolic.sym_call(lambda *x: x, *args)

def _inds_matching_labels(labels, cols):
    return [i for i,c in enumerate(cols) if c in labels]

def _labels_matching_inds(inds, cols):
    return [c for i,c in enumerate(cols) if i in inds]

def _any_cond_met(x, conditions):
    return any([c(x) for c in conditions])

def _all_cond_met(x, conditions):
    return all([c(x) for c in conditions])

def _try_except(x, f_try=_default_fn, f_except=_default_fn):
    try:
        return f_try(x)
    except:
        return f_except(x)

def _if_else(x, cond=_default_fn, f_if=_default_fn, f_else=_default_fn):
    if cond(x):
        return f_if(x)
    else:
        return f_else(x)

def _try_extract_name(x):
    return _try_except(x, _get_series_name)

def _try_extract_columns(x):
    return _try_except(x, _get_column_names)

def _if_list_make_symbolic(x):
    return _if_else(x, lambda y: isinstance(y, (list, tuple)), _list_to_symbolic)

def _repr_invert(x):
    return repr(x).count("__invert__") % 2

def _try_extract_colind(x, columns):
    return _try_except(x, lambda x: list(columns).index(x))

def _flatten_arguments(args):
    flat = []
    for arg in args:
        if isinstance(x, (list, tuple, pd.Index)):
            flat.extend(_flatten_arguments(arg))
        else:
            flat.append(arg)
    return flat


def _join_indices(index_args):
    args_ = reduce(lambda x, y: np.concatenate([np.atleast_1d(x),
                                                np.atleast_1d(y)]),
                   index_args)
    return [np.atleast_1d(args_)]



def _recursive_arg_action(arg, proc_fn=_default_fn, if_fn=_default_fn,
                          else_fn=_default_fn):
    recurse_fn = partial(_recursive_arg_action, proc_fn=proc_fn,
                         if_fn=if_fn, else_fn=else_fn)
    arg = proc_fn(arg)
    return _if_else(arg,
                    lambda a: isinstance(a, (list, tuple)),
                    if_fn([recurse_fn(a) for a in arg]),
                    else_fn(arg))


def convert_arg_to_reference(arg, df=None):
    df_eval_fn = partial(_eval_against_df, df=df)
    arg = _if_else(arg, lambda x: hasattr(x, '_eval'), df_eval_fn)
    arg = _if_else(arg, lambda x: isinstance(y, pd.Series), _get_series_name)
    arg = _if_else(arg, lambda x: isinstance(y, pd.DataFrame), _get_column_names)
    return arg



def _try_convert_to_positional(arg, columns=[]):
    return _try_except(arg, f_try=lambda x: list(columns).index(x))

def _try_convert_to_label(arg, columns=[]):
    return _try_except(arg, f_try=lambda x: list(columns)[x])


def _ref_to_labels(arg, df=None):
    proc_fn = partial(convert_arg_to_reference, df=df)
    if_fn = _list_to_symbolic
    else_fn = partial(_try_convert_to_label, columns=df.columns))
    return _recursive_arg_action(arg, proc_fn=proc_fn, if_fn=if_fn, else_fn=else_fn)


def _ref_to_positions(arg, df=None):
    proc_fn = partial(convert_arg_to_reference, df=df)
    if_fn = _list_to_symbolic
    else_fn = partial(_try_convert_to_positional, columns=df.columns)
    return _recursive_arg_action(arg, proc_fn=proc_fn, if_fn=if_fn, else_fn=else_fn)


def _arg_default_eval(arg):
    return _recursive_arg_action(arg, if_fn=_list_to_symbolic)


def _split_off_df(args):
    assert isinstance(args[0], pd.DataFrame)
    return args[0], args[1:]


def process_args(args, kwargs, arg_fn=_default_fn, kwarg_fn=_default_fn):
    args = [arg_fn(arg) for arg in args]
    kwargs = {k:kwarg_fn(v) for k,v in kwargs.items()}
    return args, kwargs


class ArgumentHandler(object):

    def __init__(self):
        arg_fn = _arg_default_eval
        kwarg_fn = _kwarg_default_eval


    def call(self, *args, **kwargs):
        self.df, args = _split_off_df(args)
        if args_as_labels:
            arg_fn = _ref_to_labels


# def embedded_function(f):
#     @wraps(f)
#     def wrapped(*args, **kwargs):
#         df, args = _split_off_df(args)
#         evaluator = partial()
#
#
#     def wrapped(*args, **kwargs):
#     #         assert len(args) > 0 and isinstance(args[0], pd.DataFrame)
#     #         if len(args) > 1:
#     #             flat_args = [args[0]]+_arg_extractor(args[1:])
#     #             return f(*flat_args, **kwargs)
#     #         else:
#     #             return f(*args, **kwargs)
#     #     return wrapped
