import pytest
from dfply import *

##==============================================================================
## desc, order by tests
##==============================================================================

def test_desc():

    df = diamonds >> select(X.cut, X.x) >> head(10)
    t = df >> summarize(last=nth(X.x, -1, order_by=[desc(X.cut), desc(X.x)]))

    series_num = pd.Series([4,1,3,2])
    series_bool = pd.Series([True,False,True,False])
    series_str = pd.Series(['d','a','c','b'])

    num_truth = series_num.rank(method='min',ascending=False)
    bool_truth = series_bool.rank(method='min',ascending=False)
    str_truth = series_str.rank(method='min',ascending=False)

    assert desc(series_num).equals(num_truth)
    assert desc(series_bool).equals(bool_truth)
    assert desc(series_str).equals(str_truth)


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


##==============================================================================
## coalesce test
##==============================================================================

def test_coalesce():
    df = pd.DataFrame({
        'a':[1,np.nan,np.nan,np.nan,np.nan],
        'b':[2,3,np.nan,np.nan,np.nan],
        'c':[np.nan,np.nan,4,5,np.nan],
        'd':[6,7,8,9,np.nan]
    })
    truth_df = df.assign(coal=[1,3,4,5,np.nan])
    d = df >> mutate(coal=coalesce(X.a, X.b, X.c, X.d))
    assert truth_df.equals(d)
