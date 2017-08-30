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
    d = diamonds >> group_by('cut','color') >> head(2)
    assert df.equals(d.reset_index(drop=True))


def test_tail():
    df = diamonds.tail(2)
    d = diamonds >> tail(2)
    assert df.equals(d)


def test_grouped_tail():
    df = diamonds.groupby(['cut','color']).apply(lambda x: x.tail(2)).reset_index(drop=True)
    d = diamonds >> group_by('cut','color') >> tail(2)
    assert df.equals(d.reset_index(drop=True))


def test_distinct():
    d = diamonds >> distinct('depth')
    df = diamonds.drop_duplicates('depth')
    assert df.equals(d)

    d = diamonds >> distinct(X.cut, 'depth')
    df = diamonds.drop_duplicates(['cut','depth'])
    assert df.equals(d)

    df = diamonds[['carat', 'cut']].drop_duplicates()
    d = diamonds >> select(X.carat, X.cut) >> distinct()
    assert df.equals(d)

    df = diamonds[['carat', 'cut']].drop_duplicates(keep='last')
    d = diamonds >> select(X.carat, X.cut) >> distinct(keep='last')
    assert df.equals(d)


def test_sample():
    random_state = 55

    d = diamonds >> sample(n=10, random_state=random_state)
    df = diamonds.sample(n=10, random_state=random_state)
    assert df.equals(d)

    d = diamonds >> sample(frac=0.001, random_state=random_state)
    df = diamonds.sample(frac=0.001, random_state=random_state)
    assert df.equals(d)

    d = diamonds >> group_by(X.cut) >> sample(n=10, random_state=random_state)
    d = d.reset_index(drop=True)
    df = diamonds.groupby('cut').apply(lambda x: x.sample(n=10, random_state=random_state))
    df = df.reset_index(drop=True)
    assert df.equals(d)


def test_row_slice():
    df = diamonds.iloc[[0,1],:]
    assert df.equals(diamonds >> row_slice([0,1]))
    df = diamonds.groupby('cut').apply(lambda df: df.iloc[0,:]).reset_index(drop=True)
    d = diamonds >> group_by(X.cut) >> row_slice(0)
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


# def test_mask_small():
#     a = (diamonds >> group_by(X.cut) >> arrange(X.price) >>
#          head(3) >> ungroup() >> mask(X.carat < 0.23))
#     print(a)
#     assert False

#  d = diamonds >> group_by(X.cut) >> mutate(price_lag=lag(X.price)) >> head(2) >> select(X.cut, X.price_lag)

def test_top_n():
    with pytest.raises(ValueError):
        diamonds >> top_n()
    test2 = diamonds >> top_n(n=6)
    df2 = diamonds.sort_values('z', ascending=False).head(6).sort_index()
    assert test2.equals(df2)
    test3 = diamonds >> top_n(col=X.x, n=5)
    df3 = diamonds.sort_values('x', ascending=False).head(5).sort_index()
    assert test3.equals(df3)
    test4 = diamonds >> top_n(col=X.cut, n=1)
    df4 = diamonds[diamonds.cut == 'Very Good']
    assert test4.equals(df4)
    test5 = diamonds >> group_by(X.cut) >> top_n(n=2)
    df5 = diamonds.ix[[27415, 27630, 23539, 27517, 27518, 24297, 24328, 24067, 25999, 26444, 48410]]
    assert test5.equals(df5)
    test6 = diamonds >> top_n(col=X.x, ascending=False, n=5)
    df6 = diamonds.sort_values('x', ascending=True).head(8).sort_index()
    assert test6.equals(df6)
