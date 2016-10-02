# dfply

The dfply package makes it possible to do R's dplyr-style data manipulation with pipes
in python on pandas DataFrames.

This package is an alternative to [pandas-ply](https://github.com/coursera/pandas-ply)
and [dplython](https://github.com/dodger487/dplython). In fact, the symbolic
representation of pandas DataFrames and Series objects for delayed calculation
is directly imported from pandas-ply.

The syntax and functionality of the package should in most cases look identical
to dplython. The major differences are in the code; dfply makes heavy use of
decorators to "categorize" the operation of data manipulation functions. The
goal of this choice of architecture is to make dfply concise and easily
extensible. There is a more in-depth overview of the decorators and how dfply can be
customized later in the readme.

<!-- START doctoc -->
<!-- END doctoc -->

## Overview

> (An ipython notebook showcasing working features of dfply [can be
found here](https://github.com/kieferk/dfply/blob/master/examples/dfply-example-gallery.ipynb))

dfply works directly on pandas DataFrames, chaining operations on the data with
the `>>` operator.

```python
from dfply import *

diamonds >> head(3)

   carat      cut color clarity  depth  table  price     x     y     z
0   0.23    Ideal     E     SI2   61.5   55.0    326  3.95  3.98  2.43
1   0.21  Premium     E     SI1   59.8   61.0    326  3.89  3.84  2.31
2   0.23     Good     E     VS1   56.9   65.0    327  4.05  4.07  2.31
```

You can chain piped operations, and of course assign the output to a new
DataFrame.

```python
lowprice = diamonds >> head(10) >> tail(3)

lowprice

   carat        cut color clarity  depth  table  price     x     y     z
7   0.26  Very Good     H     SI1   61.9   55.0    337  4.07  4.11  2.53
8   0.22       Fair     E     VS2   65.1   61.0    337  3.87  3.78  2.49
9   0.23  Very Good     H     VS1   59.4   61.0    338  4.00  4.05  2.39
```

### Selecting and dropping

A variety of selection functions are available. The current functions are:

- `select(*columns)` returns the columns specified by the arguments
- `select_containing(*substrings)` returns columns containing substrings
- `select_startswith(*substrings)` returns columns starting with substrings
- `select_endswith(*substrings)` returns columns ending with substrings
- `select_between(column1, column2)` returns columns between two specified columns (inclusive)
- `select_to(column)` returns columns up to a specified column (exclusive)
- `select_from(column)` returns columns starting from a specified column (inclusive)
- `select_through(column)` returns columns up through a specified column (exclusive)

There are complimentary dropping functions to all the selection functions,
which will remove the specified columns instead of selecting them:

- `drop(*columns)`
- `drop_containing(*substrings)`
- `drop_startswith(*substrings)`
- `drop_endswith(*substrings)`
- `drop_between(column1, column2)`
- `drop_to(column)`
- `drop_from(column)`
- `drop_through(column)`

Column selection and dropping functions are designed to work with an arbitrary
combination of string labels, positional integers, or symbolic (`X.column`)
pandas Series objects.

The functions also "flatten" their arguments so that lists or tuples of selectors
will become individual arguments. This doesn't impact the user except for the
fact that you can mix single selectors and lists of selectors as arguments.

```python
diamonds >> select(1, X.price, ['x', 'y']) >> head(2)

       cut  price     x     y
0    Ideal    326  3.95  3.98
1  Premium    326  3.89  3.84
```

```python
diamonds >> drop_endswith('e','y','z') >> head(2)

   carat      cut color  depth     x
0   0.23    Ideal     E   61.5  3.95
1   0.21  Premium     E   59.8  3.89
```

```python
diamonds >> drop_through(X.clarity) >> select_to(X.x) >> head(2)

   depth  table  price
0   61.5   55.0    326
1   59.8   61.0    326
```

### DataFrame transformation

New variables can be created with the `mutate()` function (named that to match
dplyr).

```python
diamonds >> mutate(x_plus_y=X.x + X.y) >> select_from('x') >> head(3)

      x     y     z  x_plus_y
0  3.95  3.98  2.43      7.93
1  3.89  3.84  2.31      7.73
2  4.05  4.07  2.31      8.12
```

Multiple variables can be created in a single call.

```python
diamonds >> mutate(x_plus_y=X.x + X.y, y_div_z=(X.y / X.z)) >> select_from('x') >> head(3)

      x     y     z   y_div_z  x_plus_y
0  3.95  3.98  2.43  1.637860      7.93
1  3.89  3.84  2.31  1.662338      7.73
2  4.05  4.07  2.31  1.761905      8.12
```

NOTE: because of python's unordered keyword arguments, the new variables
created with mutate are not (yet) guaranteed to be created in the same order
that they are input into the function call. This is on the todo list.

The `transmute()` function is a combination of a mutate and a selection of the
created variables.

```python
diamonds >> transmute(x_plus_y=X.x + X.y, y_div_z=(X.y / X.z)) >> head(3)

    y_div_z  x_plus_y
0  1.637860      7.93
1  1.662338      7.73
2  1.761905      8.12
```









## TODO:

1. Variables created by mutate and transform are inserted into the
DataFrame in the same order that they appear in the function call.
- A variable created first in the mutate function can be used to create a
subsequent variable in the same function call.
- Not all functions have unit tests yet.
- More complete/advanced unit tests.
- Complete and improve function documentation.
