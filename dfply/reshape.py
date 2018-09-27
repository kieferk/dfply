from .base import *
import re


# ------------------------------------------------------------------------------
# Sorting
# ------------------------------------------------------------------------------

@dfpipe
def arrange(df, *args, **kwargs):
    """Calls `pandas.DataFrame.sort_values` to sort a DataFrame according to
    criteria.

    See:
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.sort_values.html

    For a list of specific keyword arguments for sort_values (which will be
    the same in arrange).

    Args:
        *args: Symbolic, string, integer or lists of those types indicating
            columns to sort the DataFrame by.

    Kwargs:
        **kwargs: Any keyword arguments will be passed through to the pandas
            `DataFrame.sort_values` function.
    """

    flat_args = [a for a in flatten(args)]

    series = [df[arg] if isinstance(arg, str) else
              df.iloc[:, arg] if isinstance(arg, int) else
              pd.Series(arg) for arg in flat_args]

    sorter = pd.concat(series, axis=1).reset_index(drop=True)
    sorter = sorter.sort_values(sorter.columns.tolist(), **kwargs)
    return df.iloc[sorter.index, :]


# ------------------------------------------------------------------------------
# Renaming
# ------------------------------------------------------------------------------

@pipe
@symbolic_evaluation(eval_as_label=True)
def rename(df, **kwargs):
    """Renames columns, where keyword argument values are the current names
    of columns and keys are the new names.

    Args:
        df (:obj:`pandas.DataFrame`): DataFrame passed in via `>>` pipe.

    Kwargs:
        **kwargs: key:value pairs where keys are new names for columns and
            values are current names of columns.
    """

    return df.rename(columns={v: k for k, v in kwargs.items()})


# ------------------------------------------------------------------------------
# Elongate
# ------------------------------------------------------------------------------

@pipe
@symbolic_evaluation(eval_as_label=['*'])
def gather(df, key, values, *args, **kwargs):
    """
    Melts the specified columns in your DataFrame into two key:value columns.

    Args:
        key (str): Name of identifier column.
        values (str): Name of column that will contain values for the key.
        *args (str, int, symbolic): Columns to "melt" into the new key and
            value columns. If no args are specified, all columns are melted
            into they key and value columns.

    Kwargs:
        add_id (bool): Boolean value indicating whether to add a `"_ID"`
            column that will preserve information about the original rows
            (useful for being able to re-widen the data later).

    Example:
        diamonds >> gather('variable', 'value', ['price', 'depth','x','y','z']) >> head(5)

           carat      cut color clarity  table variable  value
        0   0.23    Ideal     E     SI2   55.0    price  326.0
        1   0.21  Premium     E     SI1   61.0    price  326.0
        2   0.23     Good     E     VS1   65.0    price  327.0
        3   0.29  Premium     I     VS2   58.0    price  334.0
        4   0.31     Good     J     SI2   58.0    price  335.0
    """

    if len(args) == 0:
        args = df.columns.tolist()
    else:
        args = [a for a in flatten(args)]

    if kwargs.get('add_id', False):
        df = df.assign(_ID=np.arange(df.shape[0]))

    columns = df.columns.tolist()
    id_vars = [col for col in columns if col not in args]
    return pd.melt(df, id_vars, list(args), key, values)


# ------------------------------------------------------------------------------
# Widen
# ------------------------------------------------------------------------------

def convert_type(df, columns):
    """
    Helper function that attempts to convert columns into their appropriate
    data type.
    """
    # taken in part from the dplython package
    out_df = df.copy()
    for col in columns:
        column_values = pd.Series(out_df[col].unique())
        column_values = column_values[~column_values.isnull()]
        # empty
        if len(column_values) == 0:
            continue
        # boolean
        if set(column_values.values) < {'True', 'False'}:
            out_df[col] = out_df[col].map({'True': True, 'False': False})
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


@pipe
@symbolic_evaluation(eval_as_label=['*'])
def spread(df, key, values, convert=False):
    """
    Transforms a "long" DataFrame into a "wide" format using a key and value
    column.

    If you have a mixed datatype column in your long-format DataFrame then the
    default behavior is for the spread columns to be of type `object`, or
    string. If you want to try to convert dtypes when spreading, you can set
    the convert keyword argument in spread to True.

    Args:
        key (str, int, or symbolic): Label for the key column.
        values (str, int, or symbolic): Label for the values column.

    Kwargs:
        convert (bool): Boolean indicating whether or not to try and convert
            the spread columns to more appropriate data types.


    Example:
        widened = elongated >> spread(X.variable, X.value)
        widened >> head(5)

            _ID carat clarity color        cut depth price table     x     y     z
        0     0  0.23     SI2     E      Ideal  61.5   326    55  3.95  3.98  2.43
        1     1  0.21     SI1     E    Premium  59.8   326    61  3.89  3.84  2.31
        2    10   0.3     SI1     J       Good    64   339    55  4.25  4.28  2.73
        3   100  0.75     SI1     D  Very Good  63.2  2760    56   5.8  5.75  3.65
        4  1000  0.75     SI1     D      Ideal  62.3  2898    55  5.83   5.8  3.62
    """

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
@symbolic_evaluation(eval_as_label=['*'])
def separate(df, column, into, sep="[\W_]+", remove=True, convert=False,
             extra='drop', fill='right'):
    """
    Splits columns into multiple columns.

    Args:
        df (pandas.DataFrame): DataFrame passed in through the pipe.
        column (str, symbolic): Label of column to split.
        into (list): List of string names for new columns.

    Kwargs:
        sep (str or list): If a string, the regex string used to split the
            column. If a list, a list of integer positions to split strings
            on.
        remove (bool): Boolean indicating whether to remove the original column.
        convert (bool): Boolean indicating whether the new columns should be
            converted to the appropriate type.
        extra (str): either `'drop'`, where split pieces beyond the specified
            new columns are dropped, or `'merge'`, where the final split piece
            contains the remainder of the original column.
        fill (str): either `'right'`, where `np.nan` values are filled in the
            right-most columns for missing pieces, or `'left'` where `np.nan`
            values are filled in the left-most columns.
    """

    assert isinstance(into, (tuple, list))

    if isinstance(sep, (tuple, list)):
        inds = [0] + list(sep)
        if len(inds) > len(into):
            if extra == 'drop':
                inds = inds[:len(into) + 1]
            elif extra == 'merge':
                inds = inds[:len(into)] + [None]
        else:
            inds = inds + [None]

        splits = df[column].map(lambda x: [str(x)[slice(inds[i], inds[i + 1])]
                                           if i < len(inds) - 1 else np.nan
                                           for i in range(len(into))])

    else:
        maxsplit = len(into) - 1 if extra == 'merge' else 0
        splits = df[column].map(lambda x: re.split(sep, x, maxsplit))

    right_filler = lambda x: x + [np.nan for i in range(len(into) - len(x))]
    left_filler = lambda x: [np.nan for i in range(len(into) - len(x))] + x

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

@pipe
@symbolic_evaluation(eval_as_label=['*'])
def unite(df, colname, *args, **kwargs):
    """
    Does the inverse of `separate`, joining columns together by a specified
    separator.

    Any columns that are not strings will be converted to strings.

    Args:
        df (pandas.DataFrame): DataFrame passed in through the pipe.
        colname (str): the name of the new joined column.
        *args: list of columns to be joined, which can be strings, symbolic, or
            integer positions.

    Kwargs:
        sep (str): the string separator to join the columns with.
        remove (bool): Boolean indicating whether or not to remove the
            original columns.
        na_action (str): can be one of `'maintain'` (the default),
            '`ignore'`, or `'as_string'`. The default will make the new column
            row a `NaN` value if any of the original column cells at that
            row contained `NaN`. '`ignore'` will treat any `NaN` value as an
            empty string during joining. `'as_string'` will convert any `NaN`
            value to the string `'nan'` prior to joining.
    """

    to_unite = list([a for a in flatten(args)])
    sep = kwargs.get('sep', '_')
    remove = kwargs.get('remove', True)
    # possible na_action values
    # ignore: empty string
    # maintain: keep as np.nan (default)
    # as_string: becomes string 'nan'
    na_action = kwargs.get('na_action', 'maintain')

    # print(to_unite, sep, remove, na_action)

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
