from .base import *

@label_selection
def groupby(df, *args):
    """Groups a pandas DataFrame by assigning groups to the `._grouped_by`
    attribute. The `@label_selection` decorator converts symbolic and
    positional arguments to string labels.

    Args:
        df (:obj:`pandas.DataFrame`): DataFrame to be passed in via `>>` pipe.
            This is not passed in by the user in the function call.
        *args: Columns of the DataFrame to group by. Can be stings, integer
            positions, or symbolic.

    Returns:
        Grouped DataFrame.
    """
    group_by = args
    existing_groups = getattr(df, "_grouped_by", None)

    if existing_groups is not None:
        group_by = list(set(existing_groups) | set(group_by))

    if len(group_by) > 0:
        df._grouped_by = group_by

    return df


@pipe
def ungroup(df):
    """Removes any groupings from a DataFrame by setting the `._grouped_by`
    attribute to `None`.
    """
    if getattr(df, "_grouped_by", None) is not None:
        df._grouped_by = None
    return df
