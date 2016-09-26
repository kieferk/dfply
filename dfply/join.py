from .base import *


def get_join_cols(by):
    """
    (Idea taken from dplython)
    """
    if by is None:
        return None, None
    left = [col if isinstance(col, str) else col[0] for col in by]
    right = [col if isinstance(col, str) else col[1] for col in by]
    return left, right


# def join_function(f):
#     return Pipe(
#         SymbolicReference(f)
#     )
#
# @join_function
# def inner_join(df, other, **kwargs):
#     left_on, right_on = get_join_cols(kwargs.get('by', None))
#     suffixes = kwargs.get('suffixes', ('_x', '_y'))
#     grouped_by = getattr(args[0], '_grouped_by', None)
#
#     joined = df.merge(other, how='inner', left_on=left_on,
#                       right_on=right_on, suffixes=suffixes)
#     if grouped_by is not None:
#         joined._grouped_by = grouped_by
#     return joined
