import pytest

from dfply import *

##==============================================================================
## grouping test functions
##==============================================================================


def test_group_attributes():
    d = diamonds >> group_by('cut')
    assert hasattr(d, '_grouped_by')
    assert d._grouped_by == ['cut',]

    with pytest.raises(Exception, match='unknown'):
        diamonds >> group_by('bad_col')
