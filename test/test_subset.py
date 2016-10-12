import pytest

from dfply import *


##==============================================================================
## subset test functions
##==============================================================================

def test_head():
    df = diamonds.head(2)
    d = diamonds >> head(2)
    assert df.equals(d)


def test_grouped_head():
    df = diamonds.groupby(['cut','color']).apply(lambda x: x.head(2)).reset_index(drop=True)
    d = diamonds >> groupby('cut','color') >> head(2)
    assert df.equals(d.reset_index(drop=True))


def test_tail():
    df = diamonds.tail(2)
    d = diamonds >> tail(2)
    assert df.equals(d)


def test_grouped_tail():
    df = diamonds.groupby(['cut','color']).apply(lambda x: x.tail(2)).reset_index(drop=True)
    d = diamonds >> groupby('cut','color') >> tail(2)
    assert df.equals(d.reset_index(drop=True))


def test_distinct():
    d = diamonds >> distinct('depth')
    df = diamonds.drop_duplicates('depth')
    assert df.equals(d)

    d = diamonds >> distinct(X.cut, 'depth')
    df = diamonds.drop_duplicates(['cut','depth'])
    assert df.equals(d)


def test_sample():
    random_state = 55

    d = diamonds >> sample(n=10, random_state=random_state)
    df = diamonds.sample(n=10, random_state=random_state)
    assert df.equals(d)

    d = diamonds >> sample(frac=0.001, random_state=random_state)
    df = diamonds.sample(frac=0.001, random_state=random_state)
    assert df.equals(d)

    d = diamonds >> groupby(X.cut) >> sample(n=10, random_state=random_state)
    d = d.reset_index(drop=True)
    df = diamonds.groupby('cut').apply(lambda x: x.sample(n=10, random_state=random_state))
    df = df.reset_index(drop=True)
    assert df.equals(d)


def test_row_slice():
    df = diamonds.iloc[[0,1],:]
    assert df.equals(diamonds >> row_slice([0,1]))
    df = diamonds.groupby('cut').apply(lambda df: df.iloc[0,:]).reset_index(drop=True)
    d = diamonds >> groupby(X.cut) >> row_slice(0)
    assert df.equals(d.reset_index(drop=True))
    df = diamonds.loc[diamonds.table > 61, :]
    assert df.equals(diamonds >> row_slice(X.table > 61))


def test_mask():
    test1 = diamonds >> mask(X.cut == 'Ideal')
    df = diamonds[diamonds.cut == 'Ideal']
    assert df.equals(test1)

    test2 = diamonds >> mask(X.cut == 'Ideal', X.color == 'E',
                             X.table < 55, X.price < 500)
    df_mask = (diamonds.cut == 'Ideal') & (diamonds.color == 'E')
    df_mask = df_mask & (diamonds.table < 55) & (diamonds.price < 500)
    df = diamonds[df_mask]
    assert df.equals(test2)
