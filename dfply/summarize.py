from .base import *


@dfpipe
def summarize(df, *args, **kwargs):
    for e, v in enumerate(args):
        column_name = "unnamed_arg_{}".format(e)
        if column_name not in kwargs:
            kwargs[column_name] = v
        else:
            raise KeyError(
                "Positional argument {} was assigned "
                "name '{}', which was also supplied as "
                "a keyword argument.".format(e, column_name)
            )
    return pd.DataFrame({k: [v] for k, v in kwargs.items()})


@dfpipe
def summarize_each(df, functions, *args):
    columns, values = [], []
    for arg in args:
        if isinstance(arg, pd.Series):
            varname = arg.name
            col = arg
        elif isinstance(arg, str):
            varname = arg
            col = df[varname]
        elif isinstance(arg, int):
            varname = df.columns[arg]
            col = df.iloc[:, arg]

        for f in functions:
            fname = f.__name__
            columns.append('_'.join([varname, fname]))
            values.append(f(col))

    return pd.DataFrame([values], columns=columns)
