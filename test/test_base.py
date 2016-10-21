import pytest

from dfply import *


##==============================================================================
## pipe tests
##==============================================================================

@dfpipe
def blank_function(df):
    return df


def test_pipe():
    d = diamonds >> blank_function()
    assert diamonds.equals(d)
    d = diamonds >> blank_function() >> blank_function()
    assert diamonds.equals(d)


##==============================================================================
## series/symbolic function tests
##==============================================================================

def test_desc():
    series_num = pd.Series([4,1,3,2])
    series_bool = pd.Series([True,False,True,False])
    series_str = pd.Series(['d','a','c','b'])

    assert desc(series_num).equals(series_num.rank(method='min',ascending=False))
    assert desc(series_bool).equals(series_bool.rank(method='min',ascending=False))
    assert desc(series_str).equals(series_str.rank(method='min',ascending=False))


def test_order_series_by():

    series = pd.Series([1,2,3,4,5,6,7,8])
    order1 = pd.Series(['A','B','A','B','A','B','A','B'])
    ordered1 = order_series_by(series, order1).reset_index(drop=True)
    true1 = pd.Series([1,3,5,7,2,4,6,8])
    assert ordered1.equals(true1)

    order2 = pd.Series([2,2,2,2,1,1,1,1])
    ordered2 = order_series_by(series, [order1, order2]).reset_index(drop=True)
    true2 = pd.Series([5,7,1,3,6,8,2,4])
    assert ordered2.equals(true2)
