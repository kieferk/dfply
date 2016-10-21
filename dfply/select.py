from .base import *

# ------------------------------------------------------------------------------
# Select and drop operators
# ------------------------------------------------------------------------------

@positional_selection
def select(df, *args):
    """Selects specific columns.

    Args:
        *args: Can be integers, strings, symbolic series (`X.mycol`), or lists
            of those. It can also handle pandas DataFrames, in which case the
            columns as named in that DataFrame are selected by name from df.
    """
    return df[df.columns[list(args)]]


@positional_selection
def drop(df, *args):
    """Drops specific columns.

    Args:
        *args: Can be integers, strings, symbolic series (`X.mycol`), or lists
            of those. It can also handle pandas DataFrames, in which case the
            columns as named in that DataFrame are dropped by name from df.
    """
    columns = [col for i, col in enumerate(df.columns) if not i in args]
    return df[columns]


@label_selection
def select_containing(df, *args):
    """Selects columns containing a substring or substrings.

    Args:
        *args: Integers, strings, symbolic series objects, or lists of those
            are allowed. In the case of integers and strings, the column name
            is found that corresponds to the position in the dataframe and
            that name is used as a substring.
    """
    column_matches = [
        col for col in df.columns if any([(x in col) for x in args])
    ]
    return df[column_matches]


@label_selection
def drop_containing(df, *args):
    """Drops columns containing a substring or substrings.

    Args:
        *args: Integers, strings, symbolic series objects, or lists of those
            are allowed. In the case of integers and strings, the column name
            is found that corresponds to the position in the dataframe and
            that name is used as a substring.
    """
    column_matches = [
        col for col in df.columns if any([(x in col) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@label_selection
def select_startswith(df, *args):
    """Selects columns starting with a substring or substrings.

    Args:
        *args: Integers, strings, symbolic series objects, or lists of those
            are allowed. In the case of integers and strings, the column name
            is found that corresponds to the position in the dataframe and
            that name is used as a substring.
    """
    column_matches = [
        col for col in df.columns if any([col.startswith(x) for x in args])
    ]
    return df[column_matches]


@label_selection
def drop_startswith(df, *args):
    """Drops columns starting with a substring or substrings.

    Args:
        *args: Integers, strings, symbolic series objects, or lists of those
            are allowed. In the case of integers and strings, the column name
            is found that corresponds to the position in the dataframe and
            that name is used as a substring.
    """
    column_matches = [
        col for col in df.columns if any([col.startswith(x) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@label_selection
def select_endswith(df, *args):
    """Selects columns ending with a substring or substrings.

    Args:
        *args: Integers, strings, symbolic series objects, or lists of those
            are allowed. In the case of integers and strings, the column name
            is found that corresponds to the position in the dataframe and
            that name is used as a substring.
    """
    column_matches = [
        col for col in df.columns if any([col.endswith(x) for x in args])
    ]
    return df[column_matches]


@label_selection
def drop_endswith(df, *args):
    """Drops columns ending with a substring or substrings.

    Args:
        *args: Integers, strings, symbolic series objects, or lists of those
            are allowed. In the case of integers and strings, the column name
            is found that corresponds to the position in the dataframe and
            that name is used as a substring.
    """
    column_matches = [
        col for col in df.columns if any([col.endswith(x) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@positional_selection
def select_between(df, start_index, end_index):
    """Selects columns between a starting column and ending column, inclusive.

    Args:
        start_index: Left-side starting column indicated by an integer, string
            label, or symbolic pandas series.
        end_index: Right-side ending column indicated by an integer, string
            label, or symbolic pandas series.
    """
    return df[df.columns[start_index:end_index+1]]


@positional_selection
def drop_between(df, start_index, end_index):
    """Drops columns between a starting column and ending column, inclusive.

    Args:
        start_index: Left-side starting column indicated by an integer, string
            label, or symbolic pandas series.
        end_index: Right-side ending column indicated by an integer, string
            label, or symbolic pandas series.
    """
    return df.drop(df.columns[start_index:end_index+1], axis=1)


@positional_selection
def select_from(df, start_index):
    """Selects columns starting from a specified column.

    Args:
        start_index: Left-side limit column indicated by an integer, string
            label, or symbolic pandas series.
    """
    return df[df.columns[start_index:]]


@positional_selection
def drop_from(df, start_index):
    """Drops columns starting from a specified column.

    Args:
        start_index: Left-side limit column indicated by an integer, string
            label, or symbolic pandas series.
    """
    return df.drop(df.columns[start_index:], axis=1)


@positional_selection
def select_to(df, end_index):
    """Selects columns up to a specified column (exclusive).

    Args:
        end_index: Right-side limit column indicated by an integer, string
            label, or symbolic pandas series.
    """
    return df[df.columns[:end_index]]


@positional_selection
def drop_to(df, end_index):
    """Drops columns up to a specified column (exclusive).

    Args:
        end_index: Right-side limit column indicated by an integer, string
            label, or symbolic pandas series.
    """
    return df.drop(df.columns[:end_index], axis=1)


@positional_selection
def select_through(df, end_index):
    """Selects columns up through a specified column (inclusive).

    Args:
        end_index: Right-side limit column indicated by an integer, string
            label, or symbolic pandas series.
    """
    return df[df.columns[:end_index+1]]


@positional_selection
def drop_through(df, end_index):
    """Drops columns up through a specified column (inclusive).

    Args:
        end_index: Right-side limit column indicated by an integer, string
            label, or symbolic pandas series.
    """
    return df.drop(df.columns[:end_index+1], axis=1)
