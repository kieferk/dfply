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
                             'f': [4.34, 3.95, 4.20]})
    assert t.equals(df_truth)
    # straight mutate
    t = df >> mutate(l=last(X.x))
    df_truth = df.copy()
    df_truth['l'] = df_truth.x.iloc[4]
    assert t.equals(df_truth)
    # grouped mutate
    t = df >> groupby(X.cut) >> mutate(l=last(X.x))
    df_truth['l'] = pd.Series([3.95, 4.20, 4.34, 4.20, 4.34])
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
