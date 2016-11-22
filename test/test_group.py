import pytest

from dfply import *

##==============================================================================
## grouping test functions
##==============================================================================


def test_group_attributes():
    d = diamonds >> groupby('cut')
    assert hasattr(d, '_grouped_by')
    print d.shape
    print d._grouped_by
    print d >> head(3)
    assert d._grouped_by == ['cut']
