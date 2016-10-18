import pytest

from dfply import *

##==============================================================================
## join test functions
##==============================================================================

@pytest.fixture
def dfA(scope='module'):
    a = pd.DataFrame({
        'x1':['A','B','C'],
        'x2':[1,2,3]
    })
    return a


@pytest.fixture
def dfB(scope='module'):
    b = pd.DataFrame({
        'x1':['A','B','D'],
        'x3':[True,False,True]
    })
    return b

@pytest.fixture
def dfC(scope='module'):
    c = pd.DataFrame({
        'x1':['B','C','D'],
        'x2':[2,3,4]
    })
    return c


def test_inner_join(dfA, dfB):
    ab = pd.DataFrame({
        'x1':['A','B'],
        'x2':[1,2],
        'x3':[True, False]
    })

    c = dfA >> inner_join(dfB, by='x1')
    assert c.equals(ab)


def test_outer_join(dfA, dfB):
    ab = pd.DataFrame({
        'x1':['A','B','C','D'],
        'x2':[1,2,3,np.nan],
        'x3':[True, False,np.nan,True]
    })

    c = dfA >> outer_join(dfB, by='x1')
    assert c.equals(ab)
    c = dfA >> full_join(dfB, by='x1')
    assert c.equals(ab)


def test_left_join(dfA, dfB):
    ab = pd.DataFrame({
        'x1':['A','B','C'],
        'x2':[1,2,3],
        'x3':[True, False, np.nan]
    })

    c = dfA >> left_join(dfB, by='x1')
    assert c.equals(ab)


def test_right_join(dfA, dfB):
    ab = pd.DataFrame({
        'x1':['A','B','D'],
        'x2':[1,2,np.nan],
        'x3':[True, False, True]
    })

    c = dfA >> right_join(dfB, by='x1')
    assert c.equals(ab)

def test_semi_join(dfA, dfB):
    ab = pd.DataFrame({
        'x1':['A', 'B'],
        'x2':[1, 2]
    })

    c = dfA >> semi_join(dfB, by='x1')
    assert c.equals(ab)


def test_anti_join(dfA, dfB):
    ab = pd.DataFrame({
        'x1':['C'],
        'x2':[3]
    }, index=[2])

    c = dfA >> anti_join(dfB, by='x1')
    assert c.equals(ab)


##==============================================================================
## set operation (row join) test functions
##==============================================================================

def test_union(dfA, dfC):
    ac = pd.DataFrame({
        'x1': ['A', 'B', 'C', 'D'],
        'x2': [1, 2, 3, 4]
    }, index=[0, 1, 2, 2])

    d = dfA >> union(dfC)
    assert d.equals(ac)


def test_intersect(dfA, dfC):
    ac = pd.DataFrame({
        'x1': ['B', 'C'],
        'x2': [2, 3]
    })

    d = dfA >> intersect(dfC)
    assert d.equals(ac)


def test_set_diff(dfA, dfC):
    ac = pd.DataFrame({
        'x1': ['A'],
        'x2': [1]
    })

    d = dfA >> set_diff(dfC)
    assert d.equals(ac)


##==============================================================================
## bind rows, cols
##==============================================================================

def test_bind_rows(dfA, dfB):
    inner = pd.DataFrame({
        'x1':['A','B','C','A','B','D']
    })
    outer = pd.DataFrame({
        'x1':['A','B','C','A','B','D'],
        'x2':[1,2,3,np.nan,np.nan,np.nan],
        'x3':[np.nan,np.nan,np.nan,True,False,True]
    })
    ab_inner = dfA >> bind_rows(dfB, join='inner')
    ab_outer = dfA >> bind_rows(dfB, join='outer')
    assert inner.equals(ab_inner.reset_index(drop=True))
    assert outer.equals(ab_outer.reset_index(drop=True))


def test_bind_cols(dfA, dfB):
    dfB.columns = ['x3','x4']
    df = pd.DataFrame({
        'x1':['A','B','C'],
        'x2':[1,2,3],
        'x3':['A','B','D'],
        'x4':[True,False,True]
    })
    d = dfA >> bind_cols(dfB)
    assert df.equals(d)
