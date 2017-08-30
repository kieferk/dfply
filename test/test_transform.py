import pytest

from dfply import *


##==============================================================================
## transform test functions
##==============================================================================

def test_mutate():
    df = diamonds.copy()
    df['testcol'] = 1
    assert df.equals(diamonds >> mutate(testcol=1))
    df['testcol'] = df['x']
    assert df.equals(diamonds >> mutate(testcol=X.x))
    df['testcol'] = df['x'] * df['y']
    assert df.equals(diamonds >> mutate(testcol=X.x * X.y))
    df['testcol'] = df['x'].mean()
    assert df.equals(diamonds >> mutate(testcol=np.mean(X.x)))


def group_mutate_helper(df):
    df['testcol'] = df['x']*df.shape[0]
    return df


def test_group_mutate():
    df = diamonds.copy()
    df = df.groupby('cut').apply(group_mutate_helper)
    d = diamonds >> group_by('cut') >> mutate(testcol=X.x*X.shape[0]) >> ungroup()
    assert df.equals(d.sort_index())


def test_transmute():
    df = diamonds.copy()
    df['testcol'] = df['x'] * df['y']
    df = df[['testcol']]
    assert df.equals(diamonds >> transmute(testcol=X.x * X.y))


def test_group_transmute():
    df = diamonds.copy()
    df = df.groupby('cut').apply(group_mutate_helper).reset_index(drop=True)
    df = df[['cut','testcol']]
    d = diamonds >> group_by('cut') >> transmute(testcol=X.x*X.shape[0])
    print(d.head())
    print(df.head())
    assert df.equals(d.sort_index())
