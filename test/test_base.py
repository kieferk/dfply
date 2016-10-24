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
