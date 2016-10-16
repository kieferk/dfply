from .base import *
import re


# ------------------------------------------------------------------------------
# Sorting
# ------------------------------------------------------------------------------

@pipe
@group_delegation
@symbolic_reference
@flatten_arguments
@column_indices_as_labels
def arrange(df, *args, **kwargs):
    """Calls `pandas.DataFrame.sort_values` to sort a DataFrame according to
    criteria.

    See:
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.sort_values.html

    Returns:
        Sorted DataFrame.
    """
    return df.sort_values(list(args), **kwargs)


# ------------------------------------------------------------------------------
# Renaming
# ------------------------------------------------------------------------------

@pipe
@symbolic_reference
def rename(df, **kwargs):
    """Renames columns, where keyword argument values are the current names
    of columns and keys are the new names.

    Args:
        df (:obj:`pandas.DataFrame`): DataFrame passed in via `>>` pipe.
        **kwargs: key:value pairs where keys are new names for columns and
            values are current names of columns.

    Returns:
        DataFrame with columns renamed.
    """
    return df.rename(columns={v:k for k,v in kwargs.items()})


# ------------------------------------------------------------------------------
# Elongate
# ------------------------------------------------------------------------------

@label_selection
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

def convert_type(df, columns):
    # taken in part from the dplython package
    out_df = df.copy()
    for col in columns:
        column_values = pd.Series(out_df[col].unique())
        column_values = column_values[~column_values.isnull()]
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



@label_selection
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

    out_df = (out_df >> arrange(id_cols)).reset_index(drop=True)

    return out_df


# ------------------------------------------------------------------------------
# Separate columns
# ------------------------------------------------------------------------------

@pipe
@symbolic_reference
def separate(df, column, into, sep="[\W_]+", remove=True, convert=False,
             extra='drop', fill='right'):

    assert isinstance(into, (tuple, list))

    if isinstance(sep, (tuple, list)):
        inds = [0]+list(sep)
        if len(inds) > len(into):
            if extra == 'drop':
                inds = inds[:len(into)+1]
            elif extra == 'merge':
                inds = inds[:len(into)]+[None]
        else:
            inds = inds+[None]

        splits = df[column].map(lambda x: [str(x)[slice(inds[i], inds[i+1])]
                                           if i < len(inds)-1 else np.nan
                                           for i in range(len(into))])

    else:
        maxsplit = len(into)-1 if extra == 'merge' else 0
        splits = df[column].map(lambda x: re.split(sep, x, maxsplit))

    right_filler = lambda x: x + [np.nan for i in range(len(into)-len(x))]
    left_filler = lambda x: [np.nan for i in range(len(into)-len(x))] + x

    if fill == 'right':
        splits = [right_filler(x) for x in splits]
    elif fill == 'left':
        splits = [left_filler(x) for x in splits]

    for i, split_col in enumerate(into):
        df[split_col] = [x[i] if not x[i] == '' else np.nan for x in splits]

    if convert:
        df = convert_type(df, into)

    if remove:
        df.drop(column, axis=1, inplace=True)

    return df


# ------------------------------------------------------------------------------
# Unite columns
# ------------------------------------------------------------------------------

@label_selection
def unite(df, colname, *args, **kwargs):
    to_unite = list(args)
    sep = kwargs.get('sep', '_')
    remove = kwargs.get('remove', True)
    # possible na_action values
    # ignore: empty string
    # maintain: keep as np.nan (default)
    # as_string: becomes string 'nan'
    na_action = kwargs.get('na_action', 'maintain')


    if na_action == 'maintain':
        df[colname] = df[to_unite].apply(lambda x: np.nan if any(x.isnull())
                                     else sep.join(x.map(str)), axis=1)
    elif na_action == 'ignore':
        df[colname] = df[to_unite].apply(lambda x: sep.join(x[~x.isnull()].map(str)),
                                     axis=1)
    elif na_action == 'as_string':
        df[colname] = df[to_unite].astype(str).apply(lambda x: sep.join(x), axis=1)

    if remove:
        df.drop(to_unite, axis=1, inplace=True)

    return df
