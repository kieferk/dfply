from .base import *
from .base import _arg_extractor

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

    for key, value in kwargs.items():
        df[key] = value
    return df


@dfpipe
@flatten_arguments
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
    for col in keep_columns:
        try:
            keep_cols.append(col.name)
        except:
            if isinstance(col, str):
                keep_cols.append(col)
            elif isinstance(col, int):
                keep_cols.append(df.columns[col])

    for key, value in kwargs.items():
        df[key] = value
    columns = [k for k in kwargs.keys()]+list(keep_cols)
    return df.select(lambda x: x in columns, axis=1)
