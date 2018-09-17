from .base import *


@pipe
@symbolic_evaluation(eval_as_label=True)
def group_by(df, *args):
    df._grouped_by = list(args)
    return df


@pipe
def ungroup(df):
    df._grouped_by = None
    return df
