from .base import *


def get_join_parameters(join_kwargs):
    by = kwargs.get('by', None)
    suffixes = kwargs.get('suffixes', ('_x', '_y'))
    if by is None:
        left_on, right_on = None, None
    else:
        left_on = [col if isinstance(col, str) else col[0] for col in by]
        right_on = [col if isinstance(col, str) else col[1] for col in by]
    return left_on, right_on, suffixes


@Pipe
def inner_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='inner', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined

@Pipe
def full_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='outer', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined

@Pipe
def outer_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='outer', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined

@Pipe
def left_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='left', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined

@Pipe
def right_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='right', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined
