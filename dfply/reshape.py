from .base import *


# ------------------------------------------------------------------------------
# Sorting
# ------------------------------------------------------------------------------

@args_reference_columns
def arrange(df, *args, **kwargs):
    return df.sort_values(list(args), **kwargs)


# ------------------------------------------------------------------------------
# Renaming
# ------------------------------------------------------------------------------

@kwargs_reference_columns
def rename(df, **kwargs):
    return df.rename(columns={v:k for k,v in kwargs.items()})


# ------------------------------------------------------------------------------
# Elongate
# ------------------------------------------------------------------------------

@args_reference_columns
def gather(df, key, value, *args):
    columns = df.columns.tolist()
    id_vars = [col for col in columns if col not in args]
    return pd.melt(df, id_vars, list(args), key, value)


# ------------------------------------------------------------------------------
# Widen
# ------------------------------------------------------------------------------
