from .base import *

@label_selection
def groupby(df, *args):
    group_by = args
    existing_groups = getattr(df, "_grouped_by", None)

    if existing_groups is not None:
        group_by = list(set(existing_groups) | set(group_by))

    if len(group_by) > 0:
        df_copy = df.copy()
        df_copy._grouped_by = group_by
        return df_copy
    else:
        return df

@Pipe
def ungroup(df):
    if getattr(df, "_grouped_by", None) is not None:
        df._grouped_by = None
    return df
