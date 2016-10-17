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


def IQR(series):
    iqr_s = series.quantile(.75) - series.quantile(.25)
    return iqr_s


def min(series):
    min_s = series.min()
    return min_s


def max(series):
    max_s = series.max()
    return max_s


def median(series):
    median_s = series.median()
    return median_s
