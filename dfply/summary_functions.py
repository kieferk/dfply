from .base import *


def mean(series):
    mean_s = series.mean()
    return mean_s


def first(series):
    first_s = series.iloc[0]
    return first_s


def last(series):
    last_s = series.iloc[series.size - 1]
    return last_s


def n(series):
    n_s = series.size
    return n_s


def n_distinct(series):
    n_distinct_s = series.unique().size
    return n_distinct_s
