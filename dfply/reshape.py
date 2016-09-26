from .base import *


# ------------------------------------------------------------------------------
# Sorting
# ------------------------------------------------------------------------------

@Pipe
@GroupDelegation
@SymbolicEvaluation(arg_labels=True, positional_to_labels=True)
def arrange(df, *args, **kwargs):
    return df.sort_values(list(args), **kwargs)


# ------------------------------------------------------------------------------
# Renaming
# ------------------------------------------------------------------------------

@Pipe
@SymbolicEvaluation(kwarg_labels=True)
def rename(df, **kwargs):
    return df.rename(columns={v:k for k,v in kwargs.items()})


# ------------------------------------------------------------------------------
# Elongate
# ------------------------------------------------------------------------------

@Pipe
@SymbolicEvaluation(arg_labels=True, positional_to_labels=True)
def gather(df, key, values, *args, **kwargs):
    if len(args) == 0:
        args = df.columns.tolist()

    if kwargs.get('add_id', False):
        df = df.assign(_ID=np.arange(df.shape[0]))

    columns = df.columns.tolist()
    id_vars = [col for col in columns if col not in args]
    return pd.melt(df, id_vars, list(args), key, values)


# ------------------------------------------------------------------------------
# Widen
# ------------------------------------------------------------------------------
# TODO

def convert_type(df, columns):
    # taken in part from the dplython package
    out_df = df.copy()
    for col in columns:
        column_values = pd.Series(out_df[col].unique())
        # empty
        if len(column_values) == 0:
            continue
        # boolean
        if set(column_values.values) < {'True','False'}:
            out_df[col] = out_df[col].map({'True':True, 'False':False})
            continue
        # numeric
        if pd.to_numeric(column_values, errors='coerce').isnull().sum() == 0:
            out_df[col] = pd.to_numeric(out_df[col], errors='ignore')
            continue
        # datetime
        if pd.to_datetime(column_values, errors='coerce').isnull().sum() == 0:
            out_df[col] = pd.to_datetime(out_df[col], errors='ignore',
                                         infer_datetime_format=True)
            continue

    return out_df



@Pipe
@SymbolicEvaluation(arg_labels=True, positional_to_labels=True)
def spread(df, key, values, convert=False):
    # Taken mostly from dplython package

    columns = df.columns.tolist()
    id_cols = [col for col in columns if not col in [key, values]]

    temp_index = ['' for i in range(len(df))]
    for id_col in id_cols:
        temp_index += df[id_col].map(str)

    out_df = df.assign(temp_index=temp_index)
    out_df = out_df.set_index('temp_index')
    spread_data = out_df[[key, values]]

    if not all(spread_data.groupby([spread_data.index, key]).agg(
        'count').reset_index()[values] < 2):
        raise ValueError('Duplicate identifiers')

    spread_data = spread_data.pivot(columns=key, values=values)

    if convert and (out_df[values].dtype.kind in 'OSaU'):
        columns_to_convert = [col for col in spread_data if col not in columns]
        spread_data = convert_type(spread_data, columns_to_convert)

    out_df = out_df[id_cols].drop_duplicates()
    out_df = out_df.merge(spread_data, left_index=True, right_index=True).reset_index(drop=True)
    return out_df
