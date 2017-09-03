import pytest

from dfply import *


##==============================================================================
## transform test functions
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
    d = diamonds >> group_by('cut') >> mutate(testcol=X.x*X.shape[0]) >> ungroup()
    assert df.equals(d.sort_index())


def test_transmute():
    df = diamonds.copy()
    df['testcol'] = df['x'] * df['y']
    df = df[['testcol']]
    assert df.equals(diamonds >> transmute(testcol=X.x * X.y))


def test_group_transmute():
    df = diamonds.copy()
    df = df.groupby('cut').apply(group_mutate_helper).reset_index(drop=True)
    df = df[['cut','testcol']]
    d = diamonds >> group_by('cut') >> transmute(testcol=X.x*X.shape[0])
    print(d.head())
    print(df.head())
    assert df.equals(d.sort_index())


def test_mutate_if():
    df = diamonds.copy()
    for col in df:
        try:
            if max(df[col]) < 10:
                df[col] *= 2
        except:
            pass
    assert df.equals(diamonds >> mutate_if(lambda col: max(col) < 10, lambda row: row * 2))
    df = diamonds.copy()
    for col in df:
        try:
            if any(df[col].str.contains('.')):
                df[col] = df[col].str.lower()
        except:
            pass
    assert df.equals(diamonds >> mutate_if(lambda col: any(col.str.contains('.')), lambda row: row.str.lower()))
    df = diamonds.copy()
    for col in df:
        try:
            if min(df[col]) < 1 and mean(df[col]) < 4:
                df[col] *= -1
        except:
            pass
    assert df.equals(diamonds >> mutate_if(lambda col: min(col) < 1 and mean(col) < 4, lambda row: -row))
