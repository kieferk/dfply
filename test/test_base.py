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


def test_inplace_pipe():
    df = diamonds[['price','carat']].head(5)
    d = diamonds.copy()
    d >>= select(X.price, X.carat) >> head(5)
    print(df)
    print(d)
    assert df.equals(d)
