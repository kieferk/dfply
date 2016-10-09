import pytest

from dfply.vendor.six.moves import reduce

from dfply.data import diamonds

from dfply.base import *
from dfply.group import *
from dfply.subset import *
from dfply.select import *
from dfply.reshape import *
from dfply.transform import *
from dfply.join import *
from dfply.summarize import *
from dfply.set_ops import *


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


def test_select_from():
    df = diamonds[['x','y','z']]
    assert df.equals(diamonds >> select_from('x'))
    assert df.equals(diamonds >> select_from(X.x))
    assert df.equals(diamonds >> select_from(7))

    assert diamonds[[]].equals(diamonds >> select_from(100))


def test_drop_from():
    df = diamonds[['carat','cut']]
    assert df.equals(diamonds >> drop_from('color'))
    assert df.equals(diamonds >> drop_from(X.color))
    assert df.equals(diamonds >> drop_from(2))

    assert diamonds[[]].equals(diamonds >> drop_from(0))


def test_select_to():
    df = diamonds[['carat','cut']]
    assert df.equals(diamonds >> select_to('color'))
    assert df.equals(diamonds >> select_to(X.color))
    assert df.equals(diamonds >> select_to(2))


def test_drop_to():
    df = diamonds[['x','y','z']]
    assert df.equals(diamonds >> drop_to('x'))
    assert df.equals(diamonds >> drop_to(X.x))
    assert df.equals(diamonds >> drop_to(7))


def select_through():
    df = diamonds[['carat','cut','color']]
    assert df.equals(diamonds >> select_through('color'))
    assert df.equals(diamonds >> select_through(X.color))
    assert df.equals(diamonds >> select_through(2))


def drop_through():
    df = diamonds[['y','z']]
    assert df.equals(diamonds >> drop_through('x'))
    assert df.equals(diamonds >> drop_through(X.x))
    assert df.equals(diamonds >> drop_through(7))


##==============================================================================
## subsetting
##==============================================================================

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


##==============================================================================
## reshaping
##==============================================================================

def arrange_apply_helperfunc(df):
    df = df.sort_values('depth', ascending=False)
    df = df.head(2)
    return df


def test_arrange():
    df = diamonds.groupby('cut').apply(arrange_apply_helperfunc).reset_index(drop=True)
    d = (diamonds >> groupby('cut') >> arrange('depth', ascending=False) >>
         head(2) >> ungroup()).reset_index(drop=True)

    print(df.head(5))
    print(d.head(5))
    assert df.equals(d)


def test_rename():
    df = diamonds.rename(columns={'cut':'Cut','table':'Table','carat':'Carat'})
    d = diamonds >> rename(Cut=X.cut, Table=X.table, Carat='carat')
    assert df.equals(d)


##==============================================================================
## variable creation
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
    d = diamonds >> groupby('cut') >> mutate(testcol=X.x*X.shape[0]) >> ungroup()
    assert df.equals(d)


def test_transmute():
    df = diamonds.copy()
    df['testcol'] = df['x'] * df['y']
    df = df[['testcol']]
    assert df.equals(diamonds >> transmute(testcol=X.x * X.y))


def test_group_transmute():
    df = diamonds.copy()
    df = df.groupby('cut').apply(group_mutate_helper).reset_index(drop=True)
    df = df[['cut','testcol']]
    d = diamonds >> groupby('cut') >> transmute(testcol=X.x*X.shape[0])
    assert df.equals(d)


def test_dense_rank():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_dr = df >> mutate(dr=dense_rank(X.x))
    df_truth = df
    df_truth['dr'] = pd.Series([2.0, 1.0, 3.0, 4.0, 5.0])
    assert df_dr.equals(df_truth)
    df_dr = df >> mutate(dr=dense_rank(X.cut))
    df_truth['dr'] = pd.Series([2.0, 3.0, 1.0, 3.0, 1.0])
    assert df_dr.equals(df_truth)
    df_dr = df >> groupby(X.cut) >> mutate(dr=dense_rank(X.x))
    df_truth['dr'] = pd.Series([1.0, 1.0, 1.0, 2.0, 2.0])
    assert df_dr.equals(df_truth)
    df_dr = df >> mutate(dr=dense_rank(X.x, ascending=False))
    df_truth['dr'] = pd.Series([4.0, 5.0, 3.0, 2.0, 1.0])
    assert df_dr.equals(df_truth)

def test_min_rank():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_mr = df >> mutate(mr=min_rank(X.x))
    df_truth = df
    df_truth['mr'] = pd.Series([2.0, 1.0, 3.0, 4.0, 5.0])
    assert df_mr.equals(df_truth)
    df_mr = df >> mutate(mr=min_rank(X.cut))
    df_truth['mr'] = pd.Series([3.0, 4.0, 1.0, 4.0, 1.0])
    assert df_mr.equals(df_truth)
    df_mr = df >> groupby(X.cut) >> mutate(mr=min_rank(X.x))
    df_truth['mr'] = pd.Series([1.0, 1.0, 1.0, 2.0, 2.0])
    assert df_mr.equals(df_truth)
    df_mr = df >> mutate(mr=min_rank(X.x, ascending=False))
    df_truth['mr'] = pd.Series([4.0, 5.0, 3.0, 2.0, 1.0])
    assert df_mr.equals(df_truth)


def test_cumsum():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cs = df >> mutate(cs=cumsum(X.x))
    df_truth = df
    df_truth['cs'] = pd.Series([3.95, 7.84, 11.89, 16.09, 20.43])
    assert df_cs.equals(df_truth)
    df_cs = df >> groupby(X.cut) >> mutate(cs=cumsum(X.x))
    df_truth['cs'] = pd.Series([3.95, 3.89, 4.05, 8.09, 8.39])
    assert df_cs.equals(df_truth)


def test_cummean():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cm = df >> mutate(cm=cummean(X.x))
    df_truth = df
    df_truth['cm'] = pd.Series([3.950000, 3.920000, 3.963333, 4.022500, 4.086000])
    assert df_cm.equals(df_truth)
    df_cm = df >> groupby(X.cut) >> mutate(cm=cummean(X.x))
    df_truth['cm'] = pd.Series([3.950, 3.890, 4.050, 4.045, 4.195])
    assert df_cm.equals(df_truth)


def test_cummax():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cm = df >> mutate(cm=cummax(X.x))
    df_truth = df
    df_truth['cm'] = pd.Series([3.95, 3.95, 4.05, 4.20, 4.34])
    assert df_cm.equals(df_truth)
    df_cm = df >> groupby(X.cut) >> mutate(cm=cummax(X.x))
    df_truth['cm'] = pd.Series([3.95, 3.95, 4.05, 4.20, 4.34])
    assert df_cm.equals(df_truth)


def test_cummin():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cm = df >> mutate(cm=cummin(X.x))
    df_truth = df
    df_truth['cm'] = pd.Series([3.95, 3.89, 3.89, 3.89, 3.89])
    assert df_cm.equals(df_truth)
    df_cm = df >> groupby(X.cut) >> mutate(cm=cummin(X.x))
    df_truth['cm'] = pd.Series([3.95, 3.89, 4.05, 3.89, 4.05])
    assert df_cm.equals(df_truth)


##==============================================================================
## summarization
##==============================================================================
# TODO

def test_summarize():
    p = pd.DataFrame({
        'price_mean':[diamonds.price.mean()],
        'price_std':[diamonds.price.std()]
    })
    assert p.equals(diamonds >> summarize(price_mean=X.price.mean(),
                                          price_std=X.price.std()))

    pcut = pd.DataFrame({
        'cut':['Fair','Good','Ideal','Premium','Very Good']
    })
    pcut['price_mean'] = [diamonds[diamonds.cut == c].price.mean() for c in pcut.cut.values]
    pcut['price_std'] = [diamonds[diamonds.cut == c].price.std() for c in pcut.cut.values]
    assert pcut.equals(diamonds >> groupby('cut') >>
                       summarize(price_mean=X.price.mean(), price_std=X.price.std()))


def test_summarize_each():
    to_match = pd.DataFrame({
        'price_mean':[np.mean(diamonds.price)],
        'price_var':[np.var(diamonds.price)],
        'depth_mean':[np.mean(diamonds.depth)],
        'depth_var':[np.var(diamonds.depth)]
    })
    to_match = to_match[['price_mean','price_var','depth_mean','depth_var']]

    test1 = diamonds >> summarize_each([np.mean, np.var], X.price, 4)
    test2 = diamonds >> summarize_each([np.mean, np.var], X.price, 'depth')
    assert to_match.equals(test1)
    assert to_match.equals(test2)

    group = pd.DataFrame({
        'cut':['Fair','Good','Ideal','Premium','Very Good']
    })
    group['price_mean'] = [np.mean(diamonds[diamonds.cut == c].price) for c in group.cut.values]
    group['price_var'] = [np.var(diamonds[diamonds.cut == c].price) for c in group.cut.values]
    group['depth_mean'] = [np.mean(diamonds[diamonds.cut == c].depth) for c in group.cut.values]
    group['depth_var'] = [np.var(diamonds[diamonds.cut == c].depth) for c in group.cut.values]

    group = group[['cut','price_mean','price_var','depth_mean','depth_var']]

    test1 = (diamonds >> groupby(X.cut) >>
             summarize_each([np.mean, np.var], X.price, 4))
    test2 = (diamonds >> groupby('cut') >>
             summarize_each([np.mean, np.var], X.price, 'depth'))

    assert group.equals(test1)
    assert group.equals(test2)



##==============================================================================
## joins
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
