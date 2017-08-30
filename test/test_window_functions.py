import pytest

from dfply import *

##==============================================================================
## window function tests
##==============================================================================


def test_lead():
    d = diamonds >> mutate(price_lag = lead(X.price, i=2))
    df = diamonds.assign(price_lag = diamonds.price.shift(-2))
    assert df.equals(d)


def test_lag():
    d = diamonds >> mutate(price_lag = lag(X.price, i=2))
    df = diamonds.assign(price_lag = diamonds.price.shift(2))
    assert df.equals(d)


def test_between():
    d = diamonds >> mutate(z_btwn_x_y = between(X.z, X.x, X.y))
    df = diamonds.copy()
    df['z_btwn_x_y'] = (df.z > df.x) & (df.z < df.y)
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
    df_dr = df >> group_by(X.cut) >> mutate(dr=dense_rank(X.x))
    df_truth['dr'] = pd.Series([1.0, 1.0, 1.0, 2.0, 2.0])
    assert df_dr.sort_index().equals(df_truth)
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
    df_mr = df >> group_by(X.cut) >> mutate(mr=min_rank(X.x))
    df_truth['mr'] = pd.Series([1.0, 1.0, 1.0, 2.0, 2.0])
    assert df_mr.sort_index().equals(df_truth)
    df_mr = df >> mutate(mr=min_rank(X.x, ascending=False))
    df_truth['mr'] = pd.Series([4.0, 5.0, 3.0, 2.0, 1.0])
    assert df_mr.equals(df_truth)


def test_cumsum():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cs = df >> mutate(cs=cumsum(X.x))
    df_truth = df
    df_truth['cs'] = pd.Series([3.95, 7.84, 11.89, 16.09, 20.43])
    pd.util.testing.assert_frame_equal(df_cs, df_truth)
    #assert df_cs.equals(df_truth)
    df_cs = df >> group_by(X.cut) >> mutate(cs=cumsum(X.x))
    df_truth['cs'] = pd.Series([3.95, 3.89, 4.05, 8.09, 8.39])
    pd.util.testing.assert_frame_equal(df_cs.sort_index(), df_truth)
    #assert df_cs.equals(df_truth)


def test_cummean():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cm = df >> mutate(cm=cummean(X.x))
    df_truth = df
    df_truth['cm'] = pd.Series([3.950000, 3.920000, 3.963333, 4.022500, 4.086000])
    pd.util.testing.assert_frame_equal(df_cm, df_truth)
    #assert df_cm.equals(df_truth)
    df_cm = df >> group_by(X.cut) >> mutate(cm=cummean(X.x))
    df_truth['cm'] = pd.Series([3.950, 3.890, 4.050, 4.045, 4.195])
    pd.util.testing.assert_frame_equal(df_cm.sort_index(), df_truth)
    #assert df_cm.equals(df_truth)


def test_cummax():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cm = df >> mutate(cm=cummax(X.x))
    df_truth = df
    df_truth['cm'] = pd.Series([3.95, 3.95, 4.05, 4.20, 4.34])
    pd.util.testing.assert_frame_equal(df_cm, df_truth)
    #assert df_cm.equals(df_truth)
    df_cm = df >> group_by(X.cut) >> mutate(cm=cummax(X.x))
    df_truth['cm'] = pd.Series([3.95, 3.89, 4.05, 4.20, 4.34])
    pd.util.testing.assert_frame_equal(df_cm.sort_index(), df_truth)
    #assert df_cm.equals(df_truth)


def test_cummin():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cm = df >> mutate(cm=cummin(X.x))
    df_truth = df
    df_truth['cm'] = pd.Series([3.95, 3.89, 3.89, 3.89, 3.89])
    pd.util.testing.assert_frame_equal(df_cm, df_truth)
    #assert df_cm.equals(df_truth)
    df_cm = df >> group_by(X.cut) >> mutate(cm=cummin(X.x))
    df_truth['cm'] = pd.Series([3.95, 3.89, 4.05, 3.89, 4.05])
    pd.util.testing.assert_frame_equal(df_cm.sort_index(), df_truth)
    #assert df_cm.equals(df_truth)


def test_cumprod():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cp = df >> mutate(cp=cumprod(X.x))
    df_truth = df.copy()
    df_truth['cp'] = pd.Series([3.950000, 15.365500, 62.230275, 261.367155, 1134.333453])
    pd.util.testing.assert_frame_equal(df_cp, df_truth)
    #assert df_cp.equals(df_truth)
    df_cp = df >> group_by(X.cut) >> mutate(cp=cumprod(X.x))
    df_truth['cp'] = pd.Series([3.950, 3.890, 4.050, 16.338, 17.577])
    # some tricky floating point stuff going on here
    diffs = df_cp.sort_index().cp - df_truth.cp
    assert all(diffs < .0000001)


def test_cumany():
    df = pd.DataFrame({
        'a':[False,False,True,True,False,True],
        'b':['x','x','x','x','y','y']
    })

    d = df >> mutate(ca=cumany(X.a))
    assert d.equals(df.assign(ca=[False,False,True,True,True,True]))

    d = df >> group_by(X.b) >> mutate(ca=cumany(X.a))
    assert d.sort_index().equals(df.assign(ca=[False,False,True,True,False,True]))


def test_cumall():
    df = pd.DataFrame({
        'a':[True,True,False,True,False,True],
        'b':['x','x','x','y','y','y']
    })

    d = df >> mutate(ca=cumall(X.a))
    assert d.equals(df.assign(ca=[True,True,False,False,False,False]))

    d = df >> group_by(X.b) >> mutate(ca=cumall(X.a))
    assert d.sort_index().equals(df.assign(ca=[True,True,False,True,False,False]))


def test_percent_rank():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_pr = df >> mutate(pr=percent_rank(X.x))
    df_truth = df.copy()
    assert df_pr.equals(df_truth.assign(pr=[.25, 0.00, 0.50, 0.75, 1.00]))
    df_pr = df >> mutate(pr=percent_rank(X.cut))
    assert df_pr.equals(df_truth.assign(pr=[0.50, 0.75, 0.00, 0.75, 0.00]))
    df_pr = df >> group_by(X.cut) >> mutate(pr=percent_rank(X.x))
    assert df_pr.sort_index().equals(df_truth.assign(pr=[0.0, 0.0, 0.0, 1.0, 1.0]))
    df_pr = df >> mutate(pr=percent_rank(X.x, ascending=False))
    assert df_pr.equals(df_truth.assign(pr=[0.75, 1.0, 0.50, 0.25, 0.00]))


def test_row_number():
    df = diamonds.copy().head(5).sort_values(by='x')
    df['rn'] = range(1, df.shape[0] + 1)
    df['rn'] = df['rn'].astype(float)
    df.sort_index(inplace=True)
    assert df.equals(diamonds >> head(5) >> mutate(rn=row_number(X.x)))
    # test 2: row number with desc() option
    df = diamonds.copy().head(5).sort_values(by='x', ascending=False)
    df['rn'] = range(1, df.shape[0] + 1)
    df['rn'] = df['rn'].astype(float)
    df.sort_index(inplace=True)
    assert df.equals(diamonds >> head(5) >> mutate(rn=row_number(desc(X.x))))
    # test 3: row number with ascending keyword
    df = diamonds.copy().head(5).sort_values(by='x', ascending=False)
    df['rn'] = range(1, df.shape[0] + 1)
    df['rn'] = df['rn'].astype(float)
    df.sort_index(inplace=True)
    assert df.equals(diamonds >> head(5) >> mutate(rn=row_number(X.x, ascending=False)))
    # test 4: with a group by
    df = diamonds.copy().head(5)
    df['rn'] = [1, 1, 1, 2, 2]
    df['rn'] = df['rn'].astype(float)
    assert df.equals((diamonds >> head(5) >> group_by(X.cut) >> mutate(rn=row_number(X.x))).sort_index())
