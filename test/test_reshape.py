import pytest

from dfply import *

##==============================================================================
## reshape test functions
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


@pytest.fixture
def elongated():
    elongated = diamonds >> gather('variable', 'value', add_id=True)
    return elongated


def test_gather(elongated):
    d = diamonds >> gather('variable', 'value', ['price', 'depth','x','y','z'])

    variables = ['price','depth','x','y','z']
    id_vars = [c for c in diamonds.columns if c not in variables]
    df = pd.melt(diamonds, id_vars, variables, 'variable', 'value')

    assert df.equals(d)

    d = diamonds >> gather('variable', 'value')

    variables = diamonds.columns.tolist()
    id_vars = []
    df = pd.melt(diamonds, id_vars, variables, 'variable', 'value')

    assert df.equals(d)

    df = diamonds.copy()
    df['_ID'] = np.arange(df.shape[0])
    df = pd.melt(df, ['_ID'], variables, 'variable', 'value')

    assert df.equals(elongated)


def test_spread(elongated):

    columns = elongated.columns.tolist()
    id_cols = ['_ID']

    df = elongated.copy()
    df['temp_index'] = df['_ID'].values
    df = df.set_index('temp_index')
    spread_data = df[['variable','value']]

    spread_data = spread_data.pivot(columns='variable', values='value')
    converted_spread = spread_data.copy()

    columns_to_convert = [col for col in spread_data if col not in columns]
    converted_spread = convert_type(converted_spread, columns_to_convert)

    df = df[['_ID']].drop_duplicates()

    df_spread = df.merge(spread_data, left_index=True, right_index=True).reset_index(drop=True)
    df_conv = df.merge(converted_spread, left_index=True, right_index=True).reset_index(drop=True)

    d_spread = elongated >> spread('variable', 'value')
    d_spread_conv = elongated >> spread('variable', 'value', convert=True)

    assert df_spread.equals(d_spread)
    assert df_conv.equals(d_spread_conv)


def test_separate():

    d = pd.DataFrame({
        'a':['1-a-3','1-b','1-c-3-4','9-d-1','10']
    })

    test1 = d >> separate(X.a, ['a1','a2','a3'],
                          remove=True, convert=False,
                          extra='merge', fill='right')

    true1 = pd.DataFrame({
        'a1':['1','1','1','9','10'],
        'a2':['a','b','c','d',np.nan],
        'a3':['3',np.nan,'3-4','1',np.nan]
    })
    print(test1)
    print(true1)
    assert true1.equals(test1)

    test2 = d >> separate(X.a, ['a1','a2','a3'],
                          remove=True, convert=False,
                          extra='merge', fill='left')

    true2 = pd.DataFrame({
        'a1':['1',np.nan,'1','9',np.nan],
        'a2':['a','1','c','d',np.nan],
        'a3':['3','b','3-4','1','10']
    })
    assert true2.equals(test2)

    test3 = d >> separate(X.a, ['a1','a2','a3'],
                          remove=True, convert=True,
                          extra='merge', fill='right')

    true3 = pd.DataFrame({
        'a1':[1,1,1,9,10],
        'a2':['a','b','c','d',np.nan],
        'a3':['3',np.nan,'3-4','1',np.nan]
    })
    assert true3.equals(test3)

    test4 = d >> separate(X.a, ['col1','col2'], sep=[1,3],
                          remove=True, convert=False, extra='drop', fill='left')

    true4 = pd.DataFrame({
        'col1':['1','1','1','9','1'],
        'col2':['-a','-b','-c','-d','0']
    })
    assert true4.equals(test4)

    test5 = d >> separate(X.a, ['col1','col2'], sep=[1,3],
                          remove=False, convert=False, extra='drop', fill='left')

    true5 = pd.DataFrame({
        'a':['1-a-3','1-b','1-c-3-4','9-d-1','10'],
        'col1':['1','1','1','9','1'],
        'col2':['-a','-b','-c','-d','0']
    })
    assert true5.equals(test5)

    test6 = d >> separate(X.a, ['col1','col2','col3'], sep=[30],
                          remove=True, convert=False, extra='drop', fill='left')

    true6 = pd.DataFrame({
        'col1':['1-a-3','1-b','1-c-3-4','9-d-1','10'],
        'col2':[np.nan,np.nan,np.nan,np.nan,np.nan],
        'col3':[np.nan,np.nan,np.nan,np.nan,np.nan]
    })
    assert true6.equals(test6)


def test_unite():
    d = pd.DataFrame({
        'a':[1,2,3],
        'b':['a','b','c'],
        'c':[True, False, np.nan]
    })

    test1 = d >> unite('united', X.a, 'b', 2, remove=True, na_action='maintain')
    true1 = pd.DataFrame({
        'united':['1_a_True','2_b_False',np.nan]
    })
    assert true1.equals(test1)

    test2 = d >> unite('united', ['a','b','c'], remove=True, na_action='ignore',
                       sep='*')
    true2 = pd.DataFrame({
        'united':['1*a*True','2*b*False','3*c']
    })
    assert test2.equals(true2)

    test3 = d >> unite('united', d.columns, remove=True, na_action='as_string')
    true3 = pd.DataFrame({
        'united':['1_a_True','2_b_False','3_c_nan']
    })
    assert true3.equals(test3)

    test4 = d >> unite('united', d.columns, remove=False, na_action='as_string')
    true4 = pd.DataFrame({
        'a':[1,2,3],
        'b':['a','b','c'],
        'c':[True, False, np.nan],
        'united':['1_a_True','2_b_False','3_c_nan']
    })
    assert true4.equals(test4)
