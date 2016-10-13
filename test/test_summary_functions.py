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
