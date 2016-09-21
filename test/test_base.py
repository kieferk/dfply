import pytest

from dfply.data import diamonds

from dfply.base import *
from dfply.group import *
from dfply.subset import *
from dfply.select import *


##==============================================================================
## test helper functions
##==============================================================================


@dfpipe
def blank_function(df):
    return df


##==============================================================================
## base tests
##==============================================================================


def test_pipe():
    d = diamonds >> blank_function()
    assert diamonds.equals(d)
    d = diamonds >> blank_function() >> blank_function()
    assert diamonds.equals(d)


##==============================================================================
## head and tail tests
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


##==============================================================================
## select and drop tests
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
    assert df.equals(diamonds >> select_containing('c'))
    df = diamonds[[]]
    assert df.equals(diamonds >> select_containing())


def test_drop_containing():
    df = diamonds[['depth','table','x','y','z']]
    assert df.equals(diamonds >> drop_containing('c'))


def test_select_startswith():
    df = diamonds[['carat','cut','color','clarity']]
    assert df.equals(diamonds >> select_startswith('c'))


def test_drop_startswith():
    df = diamonds[['depth','table','price','x','y','z']]
    assert df.equals(diamonds >> drop_startswith('c'))


def test_select_endswith():
    df = diamonds[['table','price']]
    assert df.equals(diamonds >> select_endswith('e'))


def test_drop_endswith():
    df = diamonds.drop('z', axis=1)
    assert df.equals(diamonds >> drop_endswith('z'))


def test_select_between():
    df = diamonds[['cut','color','clarity']]
    assert df.equals(diamonds >> select_between(X.cut, X.clarity))
    assert df.equals(diamonds >> select_between('cut', 'clarity'))
    assert df.equals(diamonds >> select_between(1, 3))

    df = diamonds[['x','y','z']]
    assert df.equals(diamonds >> select_between('x', 20))


def test_drop_between():
    df = diamonds[['carat','z']]
    assert df.equals(diamonds >> drop_between('cut','y'))
    assert df.equals(diamonds >> drop_between(X.cut, 8))

    df = diamonds[['carat','cut']]
    assert df.equals(diamonds >> drop_between(X.color, 20))


# @selectionpipe
# def select_from(df, *args):
#     ind = df.columns.tolist().index(args[0])
#     column_matches = df.columns[ind:]
#     return df[column_matches]
#
#
# @selectionpipe
# def drop_from(df, *args):
#     ind = df.columns.tolist().index(args[0])
#     column_matches = df.columns[ind:]
#     return df.drop(column_matches, axis=1)
#
#
# @selectionpipe
# def select_to(df, *args):
#     ind = df.columns.tolist().index(args[0])
#     column_matches = df.columns[:ind]
#     return df[column_matches]
#
#
# @selectionpipe
# def drop_to(df, *args):
#     ind = df.columns.tolist().index(args[0])
#     column_matches = df.columns[:ind]
#     return df.drop(column_matches, axis=1)
#
#
# @selectionpipe
# def select_through(df, *args):
#     ind = df.columns.tolist().index(args[0])
#     column_matches = df.columns.values[:ind+1]
#     return df[column_matches]
#
#
# @selectionpipe
# def drop_through(df, *args):
#     ind = df.columns.tolist().index(args[0])
#     column_matches = df.columns.values[:ind+1]
#     return df.drop(column_matches, axis=1)
