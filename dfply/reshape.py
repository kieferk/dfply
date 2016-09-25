from .base import *




# ------------------------------------------------------------------------------
# Sorting
# ------------------------------------------------------------------------------

@args_are_labels
def arrange(df, *args, **kwargs):
    return df.sort_values(list(args), **kwargs)


# ------------------------------------------------------------------------------
# Renaming
# ------------------------------------------------------------------------------

@kwargs_are_labels
def rename(df, **kwargs):
    return df.rename(columns={v:k for k,v in kwargs.items()})
