from .base import *


@selectionpipe
def select(df, *args):
    return df[list(args)]


@selectionpipe
def drop(df, *args):
    return df.drop(list(args), axis=1)


@selectionpipe
def select_containing(df, *args):
    column_matches = [
        col for col in df.columns if any([(x in col) for x in args])
    ]
    return df[column_matches]


@selectionpipe
def drop_containing(df, *args):
    column_matches = [
        col for col in df.columns if any([(x in col) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@selectionpipe
def select_startswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.startswith(x) for x in args])
    ]
    return df[column_matches]


@selectionpipe
def drop_startswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.startswith(x) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@selectionpipe
def select_endswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.endswith(x) for x in args])
    ]
    return df[column_matches]


@selectionpipe
def drop_endswith(df, *args):
    column_matches = [
        col for col in df.columns if any([col.endswith(x) for x in args])
    ]
    return df.drop(column_matches, axis=1)


@selectionpipe
def select_between(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        return df
    elif len(args) == 1:
        ind = columns.index(args[0]) if args[0] in columns else 0
        return df[columns[ind:]]
    else:
        ind1 = columns.index(args[0]) if args[0] in columns else 0
        ind2 = columns.index(args[1]) if args[1] in columns else len(columns)
        return df[columns[ind1:ind2]]


@selectionpipe
def drop_between(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        return df.drop(columns, axis=1)
    elif len(args) == 1:
        ind = columns.index(args[0]) if args[0] in columns else 0
        return df.drop(columns[ind:])
    else:
        ind1 = columns.index(args[0]) if args[0] in columns else 0
        ind2 = columns.index(args[1]) if args[1] in columns else len(columns)
        return df.drop(columns[ind1:ind2])


@selectionpipe
def select_from(df, *args):
    columns = df.columns.tolist()
    if len(args) == 0:
        return df
    else:
        ind = columns.index(args[0]) if args[0] in columns else 0
        return df[columns[ind:]]


@selectionpipe
def drop_from(df, *args):
    ind = df.columns.tolist().index(args[0])
    column_matches = df.columns[ind:]
    return df.drop(column_matches, axis=1)


@selectionpipe
def select_to(df, *args):
    ind = df.columns.tolist().index(args[0])
    column_matches = df.columns[:ind]
    return df[column_matches]


@selectionpipe
def drop_to(df, *args):
    ind = df.columns.tolist().index(args[0])
    column_matches = df.columns[:ind]
    return df.drop(column_matches, axis=1)


@selectionpipe
def select_through(df, *args):
    ind = df.columns.tolist().index(args[0])
    column_matches = df.columns.values[:ind+1]
    return df[column_matches]


@selectionpipe
def drop_through(df, *args):
    ind = df.columns.tolist().index(args[0])
    column_matches = df.columns.values[:ind+1]
    return df.drop(column_matches, axis=1)
