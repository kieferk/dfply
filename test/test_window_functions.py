import pytest

from dfply import *

##==============================================================================
## window function tests
##==============================================================================


def test_lead():
    d = diamonds >> mutate(price_lag = lag(X.price, i=2))
    df = diamonds.assign(price_lag = diamonds.price.shift(-2))
    assert df.equals(d)


def test_lag():
    d = diamonds >> mutate(price_lag = lag(X.price, i=2))
    df = diamonds.assign(price_lag = diamonds.price.shift(-2))
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
    pd.util.testing.assert_frame_equal(df_cs, df_truth)
    #assert df_cs.equals(df_truth)
    df_cs = df >> groupby(X.cut) >> mutate(cs=cumsum(X.x))
    df_truth['cs'] = pd.Series([3.95, 3.89, 4.05, 8.09, 8.39])
    pd.util.testing.assert_frame_equal(df_cs, df_truth)
    #assert df_cs.equals(df_truth)


def test_cummean():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cm = df >> mutate(cm=cummean(X.x))
    df_truth = df
    df_truth['cm'] = pd.Series([3.950000, 3.920000, 3.963333, 4.022500, 4.086000])
    pd.util.testing.assert_frame_equal(df_cm, df_truth)
    #assert df_cm.equals(df_truth)
    df_cm = df >> groupby(X.cut) >> mutate(cm=cummean(X.x))
    df_truth['cm'] = pd.Series([3.950, 3.890, 4.050, 4.045, 4.195])
    pd.util.testing.assert_frame_equal(df_cm, df_truth)
    #assert df_cm.equals(df_truth)


def test_cummax():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cm = df >> mutate(cm=cummax(X.x))
    df_truth = df
    df_truth['cm'] = pd.Series([3.95, 3.95, 4.05, 4.20, 4.34])
    pd.util.testing.assert_frame_equal(df_cm, df_truth)
    #assert df_cm.equals(df_truth)
    df_cm = df >> groupby(X.cut) >> mutate(cm=cummax(X.x))
    df_truth['cm'] = pd.Series([3.95, 3.89, 4.05, 4.20, 4.34])
    pd.util.testing.assert_frame_equal(df_cm, df_truth)
    #assert df_cm.equals(df_truth)


def test_cummin():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cm = df >> mutate(cm=cummin(X.x))
    df_truth = df
    df_truth['cm'] = pd.Series([3.95, 3.89, 3.89, 3.89, 3.89])
    pd.util.testing.assert_frame_equal(df_cm, df_truth)
    #assert df_cm.equals(df_truth)
    df_cm = df >> groupby(X.cut) >> mutate(cm=cummin(X.x))
    df_truth['cm'] = pd.Series([3.95, 3.89, 4.05, 3.89, 4.05])
    pd.util.testing.assert_frame_equal(df_cm, df_truth)
    #assert df_cm.equals(df_truth)


def test_cumprod():
    df = diamonds.copy() >> head(5) >> select(X.cut, X.x)
    df_cp = df >> mutate(cp=cumprod(X.x))
    df_truth = df.copy()
    df_truth['cp'] = pd.Series([3.950000, 15.365500, 62.230275, 261.367155, 1134.333453])
    pd.util.testing.assert_frame_equal(df_cp, df_truth)
    #assert df_cp.equals(df_truth)
    df_cp = df >> groupby(X.cut) >> mutate(cp=cumprod(X.x))
    df_truth['cp'] = pd.Series([3.950, 3.890, 4.050, 16.338, 17.577])
    # some tricky floating point stuff going on here
    diffs = df_cp.cp - df_truth.cp
    assert all(diffs < .0000001)
