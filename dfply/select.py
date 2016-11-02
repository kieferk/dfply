from .base import *
import re

# ------------------------------------------------------------------------------
# Select and drop operators
# ------------------------------------------------------------------------------

class SelectionHelper(object):

    decision_function = lambda ind, col, cols: True

    def select_function(self, df):
        return np.array([i for i,c in enumerate(df.columns)
                         if self.decision_function(i, c, df.columns)])

    def drop_function(self, df):
        return np.array([~i for i,c in enumerate(df.columns)
                         if self.decision_function(i, c, df.columns)])

    def __call__(self, *args, **kwargs):
        return self.select_function

    def __invert__(self):
        return self.select_function


@symbolic_function
def starts_with(substr, ignore_case=True):
    helper = SelectionHelper()
    if ignore_case:
        helper.decision_function = lambda ind, col, cols: col.lower().startswith(substr.lower())
    else:
        helper.decision_function = lambda ind, col, cols: col.startswith(substr)
    return helper


@symbolic_function
def ends_with(substr, ignore_case=True):
    helper = SelectionHelper()
    if ignore_case:
        helper.decision_function = lambda ind, col, cols: col.lower().endswith(substr.lower())
    else:
        helper.decision_function = lambda ind, col, cols: col.endswith(substr)
    return helper


@symbolic_function
def contains(substr, ignore_case=True):
    helper = SelectionHelper()
    if ignore_case:
        helper.decision_function = lambda ind, col, cols: substr.lower() in col.lower()
    else:
        helper.decision_function = lambda ind, col, cols: substr in col
    return helper


@symbolic_function
def matches(pattern):
    pattern = re.compile(pattern)
    helper = SelectionHelper()
    helper.decision_function = lambda ind, col, cols: pattern.search(col) is not None
    return helper


@symbolic_function
def columns_from(column):
    helper = SelectionHelper()
    if isinstance(column, int):
        helper.decision_function = lambda ind, col, cols: ind >= column
    elif isinstance(column, (pd.Series, symbolic.Expression)):
        helper.decision_function = lambda ind, col, cols: ind >= list(cols).index(column.name)
    elif isinstance(column, str):
        helper.decision_function = lambda ind, col, cols: ind >= list(cols).index(column)
    return helper


@symbolic_function
def columns_to(column):
    helper = SelectionHelper()
    if isinstance(column, int):
        helper.decision_function = lambda ind, col, cols: ind < column
    elif isinstance(column, (pd.Series, symbolic.Expression)):
        helper.decision_function = lambda ind, col, cols: ind < list(cols).index(column.name)
    elif isinstance(column, str):
        helper.decision_function = lambda ind, col, cols: ind < list(cols).index(column)
    return helper


@symbolic_function
def columns_through(column):
    helper = SelectionHelper()
    if isinstance(column, int):
        helper.decision_function = lambda ind, col, cols: ind <= column
    elif isinstance(column, (pd.Series, symbolic.Expression)):
        helper.decision_function = lambda ind, col, cols: ind <= list(cols).index(column.name)
    elif isinstance(column, str):
        helper.decision_function = lambda ind, col, cols: ind <= list(cols).index(column)
    return helper


@symbolic_function
def columns_between(start_col, end_col, inclusive=True):
    helper = SelectionHelper()
    try:
        start_col = start_col.name
    except:
        pass
    try:
        end_col = end_col.name
    except:
        pass
    indexer = lambda col, cols: col if isinstance(col, int) else list(cols).index(col)
    if inclusive:
        f = lambda ind, col, cols: ind >= indexer(start_col, cols) and ind <= indexer(end_col, cols)
    else:
        f = lambda ind, col, cols: ind > indexer(start_col, cols) and ind < indexer(end_col, cols)
    helper.decision_function = f
    return helper


@pipe
@selection
def select(df, *args):
    return df[df.columns[list(args)]]


@pipe
@selection
def drop(df, *args):
    return df.drop(df.columns[list(args)], axis=1)

# @positional_selection
# def select(df, *args):
#     """Selects specific columns.
#
#     Args:
#         *args: Can be integers, strings, symbolic series (`X.mycol`), or lists
#             of those. It can also handle pandas DataFrames, in which case the
#             columns as named in that DataFrame are selected by name from df.
#     """
#     return df[df.columns[list(args)]]


# @positional_selection
# def drop(df, *args):
#     """Drops specific columns.
#
#     Args:
#         *args: Can be integers, strings, symbolic series (`X.mycol`), or lists
#             of those. It can also handle pandas DataFrames, in which case the
#             columns as named in that DataFrame are dropped by name from df.
#     """
#     columns = [col for i, col in enumerate(df.columns) if not i in args]
#     return df[columns]


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
