import pytest
from dfply import *


##==============================================================================
## transform summary functions
##==============================================================================

def test_mean():
    df = diamonds >> select(X.cut, X.x) >> head(5)
    # straight summarize
    t = df >> summarize(m=mean(X.x))
    df_truth = pd.DataFrame({'m': [4.086]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(m=mean(X.x))
    df_truth = pd.DataFrame({'cut': ['Good', 'Ideal', 'Premium'],
                             'm': [4.195, 3.950, 4.045]})
    assert t.equals(df_truth)
    # straight mutate
    t = df >> mutate(m=mean(X.x))
    df_truth = df.copy()
    df_truth['m'] = df_truth.x.mean()
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(m=mean(X.x))
    df_truth['m'] = pd.Series([3.950, 4.045, 4.195, 4.045, 4.195])
    assert t.equals(df_truth)


def test_first():
    df = diamonds >> select(X.cut, X.x) >> head(5)
    # straight summarize
    t = df >> summarize(f=first(X.x))
    df_truth = pd.DataFrame({'f': [3.95]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(f=first(X.x))
    df_truth = pd.DataFrame({'cut': ['Good', 'Ideal', 'Premium'],
                             'f': [4.05, 3.95, 3.89]})
    assert t.equals(df_truth)
    # summarize with order_by
    t = df >> summarize(f=first(X.x, order_by=desc(X.cut)))
    df_truth = pd.DataFrame({'f':[3.89]})
    # straight mutate
    t = df >> mutate(f=first(X.x))
    df_truth = df.copy()
    df_truth['f'] = df_truth.x.iloc[0]
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(f=first(X.x))
    df_truth['f'] = pd.Series([3.95, 3.89, 4.05, 3.89, 4.05])
    assert t.equals(df_truth)


def test_last():
    df = diamonds >> select(X.cut, X.x) >> head(5)
    # straight summarize
    t = df >> summarize(l=last(X.x))
    df_truth = pd.DataFrame({'l': [4.34]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(l=last(X.x))
    df_truth = pd.DataFrame({'cut': ['Good', 'Ideal', 'Premium'],
                             'l': [4.34, 3.95, 4.20]})
    assert t.equals(df_truth)
    # summarize with order_by
    #t = df >> summarize(f=last(X.x, order_by=desc(X.cut)))
    t = df >> summarize(f=last(X.x, order_by=[desc(X.cut), desc(X.x)]))
    df_truth = pd.DataFrame({'f':[4.05]})
    assert df_truth.equals(t)
    # straight mutate
    t = df >> mutate(l=last(X.x))
    df_truth = df.copy()
    df_truth['l'] = df_truth.x.iloc[4]
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(l=last(X.x))
    df_truth['l'] = pd.Series([3.95, 4.20, 4.34, 4.20, 4.34])
    assert t.equals(df_truth)


def test_nth():
    df = diamonds >> select(X.cut, X.x) >> head(10)
    # straight summarize
    t = df >> summarize(second=nth(X.x, 1))
    df_truth = pd.DataFrame({'second': [3.89]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(first=nth(X.x, 0))
    df_truth = pd.DataFrame({'cut': ['Fair','Good', 'Ideal', 'Premium','Very Good'],
                             'first': [3.87,4.05,3.95,3.89,3.94]})
    assert t.equals(df_truth)
    # summarize with order_by
    t = df >> summarize(last=nth(X.x, -1, order_by=[desc(X.cut), desc(X.x)]))
    #print(t)
    df_truth = pd.DataFrame({'last':[3.87]})
    #print df_truth
    assert df_truth.equals(t)
    # straight mutate
    t = df >> mutate(out_of_range=nth(X.x, 500))
    df_truth = df.copy()
    df_truth['out_of_range'] = np.nan
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(penultimate=nth(X.x, -2))
    df_truth = df.copy()
    df_truth['penultimate'] = pd.Series([np.nan,3.89,4.05,3.89,4.05,4.07,
                                         4.07,4.07,np.nan,4.07])
    assert t.equals(df_truth)


def test_n():
    df = diamonds >> select(X.cut, X.x) >> head(5)
    # straight summarize
    t = df >> summarize(n=n(X.x))
    df_truth = pd.DataFrame({'n': [5]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(n=n(X.x))
    df_truth = pd.DataFrame({'cut': ['Good', 'Ideal', 'Premium'],
                             'n': [2, 1, 2]})
    assert t.equals(df_truth)
    # straight mutate
    t = df >> mutate(n=n(X.x))
    df_truth = df.copy()
    df_truth['n'] = 5
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(n=n(X.x))
    df_truth['n'] = pd.Series([1, 2, 2, 2, 2, 2])
    assert t.equals(df_truth)


def test_n_distinct():
    df = pd.DataFrame({'col_1': ['a', 'a', 'a', 'b', 'b', 'b', 'c', 'c'],
                       'col_2': [1, 1, 1, 2, 3, 3, 4, 5]})
    # straight summarize
    t = df >> summarize(n=n_distinct(X.col_2))
    df_truth = pd.DataFrame({'n': [5]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.col_1) >> summarize(n=n_distinct(X.col_2))
    df_truth = pd.DataFrame({'col_1': ['a', 'b', 'c'],
                             'n': [1, 2, 2]})
    assert t.equals(df_truth)
    # straight mutate
    t = df >> mutate(n=n_distinct(X.col_2))
    df_truth = df.copy()
    df_truth['n'] = 5
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.col_1) >> mutate(n=n_distinct(X.col_2))
    df_truth['n'] = pd.Series([1, 1, 1, 2, 2, 2, 2, 2])
    assert t.equals(df_truth)


def test_IQR():
    df = diamonds >> select(X.cut, X.x) >> head(5)
    # straight summarize
    t = df >> summarize(i=IQR(X.x))
    df_truth = pd.DataFrame({'i': [.25]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(i=IQR(X.x))
    df_truth = pd.DataFrame({'cut': ['Good', 'Ideal', 'Premium'],
                             'i': [0.145, 0.000, 0.155]})
    test_vector = abs(t.i - df_truth.i)
    assert all(test_vector < 0.000000001)
    # straight mutate
    t = df >> mutate(i=IQR(X.x))
    df_truth = df.copy()
    df_truth['i'] = 0.25
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(i=IQR(X.x))
    df_truth['i'] = pd.Series([0.000, 0.155, 0.145, 0.155, 0.145])
    test_vector = abs(t.i - df_truth.i)
    assert all(test_vector < 0.000000001)


def test_colmin():
    df = diamonds >> select(X.cut, X.x) >> head(5)
    # straight summarize
    t = df >> summarize(m=colmin(X.x))
    df_truth = pd.DataFrame({'m': [3.89]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(m=colmin(X.x))
    df_truth = pd.DataFrame({'cut': ['Good', 'Ideal', 'Premium'],
                             'm': [4.05, 3.95, 3.89]})
    assert t.equals(df_truth)
    # straight mutate
    t = df >> mutate(m=colmin(X.x))
    df_truth = df.copy()
    df_truth['m'] = 3.89
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(m=colmin(X.x))
    df_truth['m'] = pd.Series([3.95, 3.89, 4.05, 3.89, 4.05])
    assert t.equals(df_truth)


def test_colmax():
    df = diamonds >> select(X.cut, X.x) >> head(5)
    # straight summarize
    t = df >> summarize(m=colmax(X.x))
    df_truth = pd.DataFrame({'m': [4.34]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(m=colmax(X.x))
    df_truth = pd.DataFrame({'cut': ['Good', 'Ideal', 'Premium'],
                             'm': [4.34, 3.95, 4.20]})
    assert t.equals(df_truth)
    # straight mutate
    t = df >> mutate(m=colmax(X.x))
    df_truth = df.copy()
    df_truth['m'] = 4.34
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(m=colmax(X.x))
    df_truth['m'] = pd.Series([3.95, 4.20, 4.34, 4.20, 4.34])
    assert t.equals(df_truth)


def test_median():
    df = diamonds >> groupby(X.cut) >> head(3) >> select(X.cut, X.x)
    # straight summarize
    t = df >> summarize(m=median(X.x))
    df_truth = pd.DataFrame({'m': [4.05]})
    assert t.equals(df_truth)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(m=median(X.x))
    df_truth = pd.DataFrame({'cut': ['Fair', 'Good', 'Ideal', 'Premium', 'Very Good'],
                             'm': [6.27, 4.25, 3.95, 3.89, 3.95]})
    assert t.equals(df_truth)
    # straight mutate
    t = df >> mutate(m=median(X.x))
    df_truth = df.copy()
    df_truth['m'] = 4.05
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(m=median(X.x))
    df_truth['m'] = pd.Series(
        [6.27, 6.27, 6.27, 4.25, 4.25, 4.25, 3.95, 3.95, 3.95, 3.89, 3.89, 3.89, 3.95, 3.95, 3.95],
        index=t.index)
    assert t.equals(df_truth)
    # make sure it handles case with even counts properly
    df = diamonds >> groupby(X.cut) >> head(2) >> select(X.cut, X.x)
    t = df >> groupby(X.cut) >> summarize(m=median(X.x))
    df_truth = pd.DataFrame({'cut': ['Fair', 'Good', 'Ideal', 'Premium', 'Very Good'],
                             'm': [5.160, 4.195, 3.940, 4.045, 3.945]})
    test_vector = abs(t.m - df_truth.m)
    assert all(test_vector < .000000001)


def test_var():
    df = diamonds >> groupby(X.cut) >> head(3) >> select(X.cut, X.x)
    # straight summarize
    t = df >> summarize(v=var(X.x))
    df_truth = pd.DataFrame({'v': [0.687392]})
    test_vector = abs(t.v - df_truth.v)
    assert all(test_vector < .00001)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(v=var(X.x))
    df_truth = pd.DataFrame({'cut': ['Fair', 'Good', 'Ideal', 'Premium', 'Very Good'],
                             'v': [2.074800, 0.022033, 0.056133, 0.033100, 0.005233]})
    test_vector = abs(t.v - df_truth.v)
    assert all(test_vector < .00001)
    # straight mutate
    t = df >> mutate(v=var(X.x))
    df_truth = df.copy()
    df_truth['v'] = 0.687392
    test_vector = abs(t.v - df_truth.v)
    assert all(test_vector < .00001)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(v=var(X.x))
    df_truth['v'] = pd.Series([2.074800, 2.074800, 2.074800, 0.022033, 0.022033, 0.022033,
                               0.056133, 0.056133, 0.056133, 0.033100, 0.033100, 0.033100,
                               0.005233, 0.005233, 0.005233],
                              index=t.index)
    test_vector = abs(t.v - df_truth.v)
    assert all(test_vector < .00001)
    # test with single value (var undefined)
    df = diamonds >> groupby(X.cut) >> head(1) >> select(X.cut, X.x)
    t = df >> groupby(X.cut) >> summarize(v=var(X.x))
    df_truth = pd.DataFrame({'cut': ['Fair', 'Good', 'Ideal', 'Premium', 'Very Good'],
                             'v': [np.nan, np.nan, np.nan, np.nan, np.nan]})
    assert t.equals(df_truth)


def test_sd():
    df = diamonds >> groupby(X.cut) >> head(3) >> select(X.cut, X.x)
    # straight summarize
    t = df >> summarize(s=sd(X.x))
    df_truth = pd.DataFrame({'s': [0.829091]})
    test_vector = abs(t.s - df_truth.s)
    assert all(test_vector < .00001)
    # grouped summarize
    t = df >> groupby(X.cut) >> summarize(s=sd(X.x))
    df_truth = pd.DataFrame({'cut': ['Fair', 'Good', 'Ideal', 'Premium', 'Very Good'],
                             's': [1.440417, 0.148436, 0.236925, 0.181934, 0.072342]})
    test_vector = abs(t.s - df_truth.s)
    assert all(test_vector < .00001)
    # straight mutate
    t = df >> mutate(s=sd(X.x))
    df_truth = df.copy()
    df_truth['s'] = 0.829091
    test_vector = abs(t.s - df_truth.s)
    assert all(test_vector < .00001)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(s=sd(X.x))
    df_truth['s'] = pd.Series([1.440417, 1.440417, 1.440417, 0.148436, 0.148436, 0.148436,
                               0.236925, 0.236925, 0.236925, 0.181934, 0.181934, 0.181934,
                               0.072342, 0.072342, 0.072342],
                              index=t.index)
    test_vector = abs(t.s - df_truth.s)
    assert all(test_vector < .00001)
    # test with single value (var undefined)
    df = diamonds >> groupby(X.cut) >> head(1) >> select(X.cut, X.x)
    t = df >> groupby(X.cut) >> summarize(s=sd(X.x))
    df_truth = pd.DataFrame({'cut': ['Fair', 'Good', 'Ideal', 'Premium', 'Very Good'],
                             's': [np.nan, np.nan, np.nan, np.nan, np.nan]})
    assert t.equals(df_truth)
