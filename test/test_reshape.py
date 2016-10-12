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
