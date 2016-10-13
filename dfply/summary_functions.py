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
