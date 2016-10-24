from .base import *

# ------------------------------------------------------------------------------
# series ordering
# ------------------------------------------------------------------------------

@make_symbolic
def order_series_by(series, order_series):
    """Orders one series according to another series, or a list of other
    series. If a list of other series are specified, ordering is done hierarchically
    like when a list of columns is supplied to `.sort_values()`.

    Args:
        series (:obj:`pandas.Series`): the pandas Series object to be reordered.
        order_series: either a pandas Series object or a list of pandas Series
            objects. These will be sorted using `.sort_values()` with
            `ascending=True`, and the new order will be used to reorder the
            Series supplied in the first argument.

    Returns:
        reordered `pandas.Series` object

    """

    if isinstance(order_series, (list, tuple)):
        sorter = pd.concat(order_series, axis=1)
        sorter_columns = ['_sorter'+str(i) for i in range(len(order_series))]
        sorter.columns = sorter_columns
        sorter['series'] = series.values
        sorted_series = sorter.sort_values(sorter_columns)['series']
        return sorted_series
    else:
        sorted_series = pd.DataFrame({
            'series':series.values,
            'order':order_series.values
        }).sort_values('order', ascending=True)['series']
        return sorted_series


@make_symbolic
def desc(series):
    """Mimics the functionality of the R desc function. Essentially inverts a
    series object to make ascending sort act like descending sort.

    Example:

    First group by cut, then find the first value of price when ordering by
    price ascending, and ordering by price descending using the `desc` function.

    diamonds >> group_by(X.cut) >> summarize(carat_low=first(X.price, order_by=X.price),
                                             carat_high=first(X.price, order_by=desc(X.price)))

             cut  carat_high  carat_low
    0       Fair       18574        337
    1       Good       18788        327
    2      Ideal       18806        326
    3    Premium       18823        326
    4  Very Good       18818        336

    Args:
        series (:obj:`pandas.Series`): pandas series to be inverted prior to
            ordering/sorting.

    Returns:
        inverted `pandas.Series`. The returned series will be numeric (integers),
            regardless of the type of the original series.

    """
    return series.rank(method='min', ascending=False)



# ------------------------------------------------------------------------------
# coalesce
# ------------------------------------------------------------------------------

@make_symbolic
def coalesce(*series):
    series = [pd.Series(s) for s in series]
    coalescer = pd.concat(series, axis=1)
    min_nonna = np.argmin(pd.isnull(coalescer).values, axis=1)
    min_nonna = [coalescer.columns[i] for i in min_nonna]
    return coalescer.lookup(np.arange(coalescer.shape[0]), min_nonna)
