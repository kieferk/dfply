from .base import *


@dfpipe
def mutate(df, **kwargs):
    """
    Creates new variables (columns) in the DataFrame specified by keyword
    argument pairs, where the key is the column name and the value is the
    new column value(s).

    Args:
        df (pandas.DataFrame): data passed in through the pipe.

    Kwargs:
        **kwargs: keys are the names of the new columns, values indicate
            what the new column values will be.

    Example:
        diamonds >> mutate(x_plus_y=X.x + X.y) >> select_from('x') >> head(3)

              x     y     z  x_plus_y
        0  3.95  3.98  2.43      7.93
        1  3.89  3.84  2.31      7.73
        2  4.05  4.07  2.31      8.12
    """

    return df.assign(**kwargs)


@dfpipe
def mutate_if(df, predicate, fun):
    """
    Modifies columns in place if the specified predicate is true.
    Args:
        df (pandas.DataFrame): data passed in through the pipe.
        predicate: a function applied to columns that returns a boolean value
        fun: a function that will be applied to columns where predicate returns True

    Example:
        diamonds >> mutate_if(lambda col: min(col) < 1 and mean(col) < 4, lambda row: 2 * row) >> head(3)
           carat      cut color clarity  depth  table  price     x     y     z
        0   0.46    Ideal     E     SI2   61.5   55.0    326  3.95  3.98  4.86
        1   0.42  Premium     E     SI1   59.8   61.0    326  3.89  3.84  4.62
        2   0.46     Good     E     VS1   56.9   65.0    327  4.05  4.07  4.62
        (columns 'carat' and 'z', both having a min < 1 and mean < 4, are doubled, while the
        other rows remain as they were)
    """
    cols = list()
    for col in df:
        try:
            if predicate(df[col]):
                cols.append(col)
        except:
            pass
    df[cols] = df[cols].apply(fun)
    return df

    # df2 = df.copy()
    # df2[cols] = df2[cols].apply(fun)
    # return df2


@dfpipe
def transmute(df, *keep_columns, **kwargs):
    """
    Creates columns and then returns those new columns and optionally specified
    original columns from the DataFrame.

    This works like `mutate`, but designed to discard the original columns used
    to create the new ones.

    Args:
        *keep_columns: Column labels to keep. Can be string, symbolic, or
            integer position.

    Kwargs:
        **kwargs: keys are the names of the new columns, values indicate
            what the new column values will be.

    Example:
        diamonds >> transmute(x_plus_y=X.x + X.y, y_div_z=(X.y / X.z)) >> head(3)

            y_div_z  x_plus_y
        0  1.637860      7.93
        1  1.662338      7.73
        2  1.761905      8.12
    """

    keep_cols = []
    for col in flatten(keep_columns):
        try:
            keep_cols.append(col.name)
        except:
            if isinstance(col, str):
                keep_cols.append(col)
            elif isinstance(col, int):
                keep_cols.append(df.columns[col])

    df = df.assign(**kwargs)
    columns = [k for k in kwargs.keys()] + list(keep_cols)
    return df[columns]
