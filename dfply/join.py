from .base import *

# ------------------------------------------------------------------------------
# SQL-style joins
# ------------------------------------------------------------------------------

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


@pipe
def semi_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    if not right_on:
        right_on = [col_name for col_name in df.columns.values.tolist() if col_name in other.columns.values.tolist()]
        left_on = right_on
    else:
        right_on = [right_on]
    other_reduced = other[right_on].drop_duplicates()
    joined = df.merge(other_reduced, how='inner', left_on=left_on,
                      right_on=right_on, suffixes=('', '_y'),
                      indicator=True).query('_merge=="both"')[df.columns.values.tolist()]
    return joined


@pipe
def anti_join(df, other, **kwargs):
    left_on, right_on, suffixes = get_join_parameters(kwargs)
    if not right_on:
        right_on = [col_name for col_name in df.columns.values.tolist() if col_name in other.columns.values.tolist()]
        left_on = right_on
    else:
        right_on = [right_on]
    other_reduced = other[right_on].drop_duplicates()
    joined = df.merge(other_reduced, how='left', left_on=left_on,
                      right_on=right_on, suffixes=('', '_y'),
                      indicator=True).query('_merge=="left_only"')[df.columns.values.tolist()]
    return joined


# ------------------------------------------------------------------------------
# Binding
# ------------------------------------------------------------------------------

@pipe
def bind_rows(df, other, join='outer', ignore_index=False):
    df = pd.concat([df, other], join=join, ignore_index=ignore_index, axis=0)
    return df


@pipe
def bind_cols(df, other, join='outer', ignore_index=False):
    df = pd.concat([df, other], join=join, ignore_index=ignore_index, axis=1)
    return df
