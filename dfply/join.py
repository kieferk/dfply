from .base import *


def get_join_parameters(join_kwargs):
    by = join_kwargs.get('by', None)
    suffixes = join_kwargs.get('suffixes', ('_x', '_y'))
    if by is None:
        left_on, right_on = None, None
    else:
        if isinstance(by, str):
            left_on, right_on = by, by
        else:
            left_on = [col if isinstance(col, str) else col[0] for col in by]
            right_on = [col if isinstance(col, str) else col[1] for col in by]
    return left_on, right_on, suffixes


@pipe
def inner_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='inner', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined

@pipe
def full_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='outer', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined

@pipe
def outer_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='outer', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined

@pipe
def left_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='left', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined

@pipe
def right_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    joined = df.merge(other, how='right', left_on=left_on,
                      right_on=right_on, suffixes=suffixes)
    return joined
