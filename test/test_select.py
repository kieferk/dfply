import pytest

from dfply import *

##==============================================================================
## select and drop test functions
##==============================================================================

#       0     1      2     3       4      5      6      7     8     9
#   carat    cut color clarity  depth  table  price     x     y     z
#    0.23  Ideal     E     SI2   61.5   55.0    326  3.95  3.98  2.43

def test_select():
    df = diamonds[['carat','cut','price']]
    assert df.equals(diamonds >> select('carat','cut','price'))
    assert df.equals(diamonds >> select(0, 1, 6))
    assert df.equals(diamonds >> select(0, 1, 'price'))
    assert df.equals(diamonds >> select([0, X.cut], X.price))
    assert df.equals(diamonds >> select(X.carat, X['cut'], X.price))
    assert df.equals(diamonds >> select(X[['carat','cut','price']]))
    assert df.equals(diamonds >> select(X[['carat','cut']], X.price))
    assert df.equals(diamonds >> select(X.iloc[:,[0,1,6]]))
    assert df.equals(diamonds >> select([X.loc[:, ['carat','cut','price']]]))


def test_drop():
    df = diamonds.drop(['carat','cut','price'], axis=1)
    assert df.equals(diamonds >> drop('carat','cut','price'))
    assert df.equals(diamonds >> drop(0, 1, 6))
    assert df.equals(diamonds >> drop(0, 1, 'price'))
    assert df.equals(diamonds >> drop([0, X.cut], X.price))
    assert df.equals(diamonds >> drop(X.carat, X['cut'], X.price))
    assert df.equals(diamonds >> drop(X[['carat','cut','price']]))
    assert df.equals(diamonds >> drop(X[['carat','cut']], X.price))
    assert df.equals(diamonds >> drop(X.iloc[:,[0,1,6]]))
    assert df.equals(diamonds >> drop([X.loc[:, ['carat','cut','price']]]))


def test_select_containing():
    df = diamonds[['carat','cut','color','clarity','price']]
    assert df.equals(diamonds >> select(contains('c')))


def test_drop_containing():
    df = diamonds[['depth','table','x','y','z']]
    assert df.equals(diamonds >> drop(contains('c')))


def test_select_startswith():
    df = diamonds[['carat','cut','color','clarity']]
    assert df.equals(diamonds >> select(starts_with('c')))


def test_drop_startswith():
    df = diamonds[['depth','table','price','x','y','z']]
    assert df.equals(diamonds >> drop(starts_with('c')))


def test_select_endswith():
    df = diamonds[['table','price']]
    assert df.equals(diamonds >> select(ends_with('e')))


def test_drop_endswith():
    df = diamonds.drop('z', axis=1)
    assert df.equals(diamonds >> drop(ends_with('z')))


def test_select_between():
    df = diamonds[['cut','color','clarity']]
    assert df.equals(diamonds >> select(columns_between(X.cut, X.clarity)))
    assert df.equals(diamonds >> select(columns_between('cut', 'clarity')))
    assert df.equals(diamonds >> select(columns_between(1, 3)))

    df = diamonds[['x','y','z']]
    assert df.equals(diamonds >> select(columns_between('x', 20)))



def test_drop_between():
    df = diamonds[['carat','z']]
    assert df.equals(diamonds >> drop(columns_between('cut','y')))
    assert df.equals(diamonds >> drop(columns_between(X.cut, 8)))

    df = diamonds[['carat','cut']]
    assert df.equals(diamonds >> drop(columns_between(X.color, 20)))


def test_select_from():
    df = diamonds[['x','y','z']]
    assert df.equals(diamonds >> select(columns_from('x')))
    assert df.equals(diamonds >> select(columns_from(X.x)))
    assert df.equals(diamonds >> select(columns_from(7)))

    assert diamonds[[]].equals(diamonds >> select(columns_from(100)))


def test_drop_from():
    df = diamonds[['carat','cut']]
    assert df.equals(diamonds >> drop(columns_from('color')))
    assert df.equals(diamonds >> drop(columns_from(X.color)))
    assert df.equals(diamonds >> drop(columns_from(2)))

    #print(diamonds >> drop(columns_from(0)))
    assert diamonds[[]].equals(diamonds >> drop(columns_from(0)))


def test_select_to():
    df = diamonds[['carat','cut']]
    assert df.equals(diamonds >> select(columns_to('color')))
    assert df.equals(diamonds >> select(columns_to(X.color)))
    assert df.equals(diamonds >> select(columns_to(2)))


def test_drop_to():
    df = diamonds[['x','y','z']]
    assert df.equals(diamonds >> drop(columns_to('x')))
    assert df.equals(diamonds >> drop(columns_to(X.x)))
    assert df.equals(diamonds >> drop(columns_to(7)))


def select_through():
    df = diamonds[['carat','cut','color']]
    assert df.equals(diamonds >> select(columns_to('color', inclusive=True)))
    assert df.equals(diamonds >> select(columns_to(X.color, inclusive=True)))
    assert df.equals(diamonds >> select(columns_to(2, inclusive=True)))


def drop_through():
    df = diamonds[['y','z']]
    assert df.equals(diamonds >> drop(columns_to('x', inclusive=True)))
    assert df.equals(diamonds >> drop(columns_to(X.x, inclusive=True)))
    assert df.equals(diamonds >> drop(columns_to(7, inclusive=True)))
