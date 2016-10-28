from .base import *
from .base import _col_ind_to_position


# ------------------------------------------------------------------------------
# Select and drop operators
# ------------------------------------------------------------------------------

@selection_helper
def starts_with(match, ignore_case=True):
    if ignore_case:
        matches = lambda df: np.array([i+1 for i,c in enumerate(df.columns)
                                       if c.lower().startswith(match.lower())])
    else:
        matches = lambda df: np.array([i+1 for i,c in enumerate(df.columns)
                                       if c.startswith(match)])
    return matches
    #return symbolic.sym_call(col_check, X)

# def selection_helper(f):
#     @wraps(f)
#     def wrapped(*args, **kwargs):
#         assert (len(args) > 0) and (isinstance(args[0], pd.DataFrame))
#         columns = args[0].columns.tolist()
#         matches = [[i,c] for i,c in enumerate(columns)]
#
#         str_to_list = lambda s: [s] if isinstance(s, str) else s
#
#         label_keep = lambda m, f, x: m if x is None else [[i,c] for i,c in m if f(c, x)]
#         label_remove = lambda m, f, x: m if x is None else [[i,c] for i,c in m if not f(c, x)]
#
#         position_keep = lambda m, f, x: m if x is None else [[i,c] for i,c in m if f(i, x)]
#         position_remove = lambda m, f, x: m if x is None else [[i,c] for i,c in m if not f(i, x)]
#
#         label_contains = lambda label, containing: any([(x in label) for x in containing])
#         label_startswith = lambda label, substr: any([label.startswith(x) for x in substr])
#         label_endswith = lambda label, substr: any([label.endswith(x) for x in substr])
#
#         position_between = lambda pos, window: (pos >= window[0]) and (pos <= window[1])
#         position_from = lambda pos, start: (pos >= start)
#         position_to = lambda pos, finish: (pos < finish)
#         position_through = lambda pos, finish: (pos <= finish)
#
#         containing = str_to_list(kwargs.get('containing', None))
#         matches = label_keep(matches, label_contains, containing)
#
#         not_containing = str_to_list(kwargs.get('not_containing', None))
#         matches = label_remove(matches, label_contains, not_containing)
#
#         startingwith = str_to_list(kwargs.get('startingwith', None))
#         matches = label_keep(matches, label_startswith, startingwith)
#
#         not_startingwith = str_to_list(kwargs.get('not_startingwith', None))
#         matches = label_remove(matches, label_startswith, not_startingwith)
#
#         endingwith = str_to_list(kwargs.get('endingwith', None))
#         matches = label_keep(matches, label_endswith, endingwith)
#
#         not_endingwith = str_to_list(kwargs.get('not_endingwith', None))
#         matches = label_remove(matches, label_endswith, not_endingwith)
#
#         between = kwargs.get('between', None)
#         matches = position_keep(matches, position_between, between)
#
#         not_between = kwargs.get('not_between', None)
#         matches = position_remove(matches, position_between, not_between)
#
#         pos_from = kwargs.get('from', None)
#         matches = position_keep(matches, position_from, pos_from)
#
#         not_pos_from = kwargs.get('not_from', None)
#         matches = position_remove(matches, position_from, not_pos_from)
#
#         pos_to = kwargs.get('to', None)
#         matches = position_keep(matches, position_to, pos_to)
#
#         not_pos_to = kwargs.get('not_to', None)
#         matches = position_remove(matches, position_to, not_pos_to)
#
#         pos_through = kwargs.get('through', None)
#         matches = position_keep(matches, position_through, pos_through)
#
#         not_pos_through = kwargs.get('not_through', None)
#         matches = position_remove(matches, position_through, not_pos_through)
#
#         return f(*args, **kwargs)
#     return wrapped


@pipe
@selection_helper
def select(df, *args):
    return df[df.columns[list(args)]]

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
