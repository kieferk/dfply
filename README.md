# dfply

### Version: 0.1.9

The dfply package makes it possible to do R's dplyr-style data manipulation with pipes
in python on pandas DataFrames.

This is an alternative to [pandas-ply](https://github.com/coursera/pandas-ply)
and [dplython](https://github.com/dodger487/dplython). It is heavily inspired by
both of them, and in fact the code for symbolic representation of pandas DataFrames
and series (e.g. `X.variable`) is imported from pandas-ply.

The syntax and functionality of the package will in most cases be identical
to dplyr and dplython. dfply uses a decorator-based structure for piping and categorizing
data manipulation functions. The goal of the decorator architecture is to make dfply concise and easily
extensible. There is a more in-depth overview of the decorators and how dfply can be
customized below.

Dfply is intended to mimic the functionality of dplyr. The syntax, while most often
the same, varies in cases where it makes more sense to do it differently in python.


**Expect frequent updates to the package version as features are added and
any bugs are fixed.**

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Overview](#overview)
  - [The `>>` pipe operator](#the--pipe-operator)
  - [The pandas-ply `X` DataFrame symbol](#the-pandas-ply-x-dataframe-symbol)
  - [Selecting and dropping](#selecting-and-dropping)
    - [`select*()` and `drop*()` functions](#select-and-drop-functions)
  - [Subsetting and filtering](#subsetting-and-filtering)
    - [`row_slice()`](#row_slice)
    - [`sample()`](#sample)
    - [`distinct()`](#distinct)
    - [`mask()`](#mask)
  - [DataFrame transformation](#dataframe-transformation)
    - [`mutate()`](#mutate)
    - [`transmute()`](#transmute)
  - [Grouping](#grouping)
    - [`groupby()` and `ungroup()`](#groupby-and-ungroup)
  - [Reshaping](#reshaping)
    - [`arrange()`](#arrange)
    - [`rename()`](#rename)
    - [`gather()`](#gather)
    - [`spread()`](#spread)
    - [`separate()`](#separate)
    - [`unite()`](#unite)
  - [Joining](#joining)
    - [`inner_join()`](#inner_join)
    - [`outer_join()` or `full_join()`](#outer_join-or-full_join)
    - [`left_join()`](#left_join)
    - [`right_join()`](#right_join)
    - [`semi_join()`](#semi_join)
    - [`anti_join()`](#anti_join)
  - [Set operations](#set-operations)
    - [`union()`](#union)
    - [`intersect()`](#intersect)
    - [`set_diff()`](#set_diff)
  - [Binding](#binding)
    - [`bind_rows()`](#bind_rows)
    - [`bind_cols()`](#bind_cols)
  - [Summarization](#summarization)
    - [`summarize()`](#summarize)
    - [`summarize_each()`](#summarize_each)
- [Decorators](#decorators)
  - [`@pipe`](#pipe)
  - [`@group_delegation`](#group_delegation)
  - [`@symbolic_evaluation` and `@symbolic_reference`](#symbolic_evaluation-and-symbolic_reference)
  - [`@dfpipe`](#dfpipe)
  - [Extending and mixing behavior with decorators](#extending-and-mixing-behavior-with-decorators)
- [Contributing](#contributing)
- [TODO:](#todo)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

> (A notebook showcasing most of the working functions in dfply [can be
found here](https://github.com/kieferk/dfply/blob/master/examples/dfply-example-gallery.ipynb))

### The `>>` pipe operator

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

### The pandas-ply `X` DataFrame symbol

The DataFrame as it is passed through the piping operations is represented
by the symbol `X`. This functionality is imported from pandas-ply, and allows
operations on the DataFrame to be deferred until the appropriate time. Selecting
two of the columns, for example, can be done using the symbolic `X` DataFrame
during the piping operations.

```python
diamonds >> select(X.carat, X.cut) >> head(3)

   carat      cut
0   0.23    Ideal
1   0.21  Premium
2   0.23     Good
```


### Selecting and dropping

#### `select*()` and `drop*()` functions

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

### Subsetting and filtering

#### `row_slice()`

Slices of rows can be selected with the `row_slice()` function. You can pass
single integer indices and/or lists of integer indices to select rows as with
pandas' `.iloc`.

```python
diamonds >> row_slice(1,2,[10,15])

    carat      cut color clarity  depth  table  price     x     y     z
1    0.21  Premium     E     SI1   59.8   61.0    326  3.89  3.84  2.31
2    0.23     Good     E     VS1   56.9   65.0    327  4.05  4.07  2.31
10   0.30     Good     J     SI1   64.0   55.0    339  4.25  4.28  2.73
15   0.32  Premium     E      I1   60.9   58.0    345  4.38  4.42  2.68
```

#### `sample()`

The `sample()` function functions exactly the same as pandas' `.sample()` method
for DataFrames. Arguments and keyword arguments will be passed through to the
DataFrame sample method.

```python
diamonds >> sample(frac=0.0001, replace=False)

       carat        cut color clarity  depth  table  price     x     y     z
19736   1.02      Ideal     E     VS1   62.2   54.0   8303  6.43  6.46  4.01
37159   0.32    Premium     D     VS2   60.3   60.0    972  4.44  4.42  2.67
1699    0.72  Very Good     E     VS2   63.8   57.0   3035  5.66  5.69  3.62
20955   1.71  Very Good     J     VS2   62.6   55.0   9170  7.58  7.65  4.77
5168    0.91  Very Good     E     SI2   63.0   56.0   3772  6.12  6.16  3.87


diamonds >> sample(n=3, replace=True)

       carat        cut color clarity  depth  table  price     x     y     z
52892   0.73  Very Good     G     SI1   60.6   59.0   2585  5.83  5.85  3.54
39454   0.57      Ideal     H     SI2   62.3   56.0   1077  5.31  5.28  3.30
39751   0.43      Ideal     H    VVS1   62.3   54.0   1094  4.84  4.85  3.02
```

#### `distinct()`

Selection of unique rows is done with `distinct()`, which similarly passes
arguments and keyword arguments through to the DataFrame's `.drop_duplicates()`
method.

```python
diamonds >> distinct(X.color)

    carat        cut color clarity  depth  table  price     x     y     z
0    0.23      Ideal     E     SI2   61.5   55.0    326  3.95  3.98  2.43
3    0.29    Premium     I     VS2   62.4   58.0    334  4.20  4.23  2.63
4    0.31       Good     J     SI2   63.3   58.0    335  4.34  4.35  2.75
7    0.26  Very Good     H     SI1   61.9   55.0    337  4.07  4.11  2.53
12   0.22    Premium     F     SI1   60.4   61.0    342  3.88  3.84  2.33
25   0.23  Very Good     G    VVS2   60.4   58.0    354  3.97  4.01  2.41
28   0.23  Very Good     D     VS2   60.5   61.0    357  3.96  3.97  2.40
```

#### `mask()`

Filtering rows with logical criteria is done with `mask()`, which accepts
boolean arrays "masking out" False labeled rows and keeping True labeled rows.
These are best created with logical statements on symbolic Series objects as
shown below. Multiple criteria can be supplied as arguments and their intersection
will be used as the mask.

```python
diamonds >> mask(X.cut == 'Ideal') >> head(4)

    carat    cut color clarity  depth  table  price     x     y     z
0    0.23  Ideal     E     SI2   61.5   55.0    326  3.95  3.98  2.43
11   0.23  Ideal     J     VS1   62.8   56.0    340  3.93  3.90  2.46
13   0.31  Ideal     J     SI2   62.2   54.0    344  4.35  4.37  2.71
16   0.30  Ideal     I     SI2   62.0   54.0    348  4.31  4.34  2.68

diamonds >> mask(X.cut == 'Ideal', X.color == 'E', X.table < 55, X.price < 500)

       carat    cut color clarity  depth  table  price     x     y     z
26683   0.33  Ideal     E     SI2   62.2   54.0    427  4.44  4.46  2.77
32297   0.34  Ideal     E     SI2   62.4   54.0    454  4.49  4.52  2.81
40928   0.30  Ideal     E     SI1   61.6   54.0    499  4.32  4.35  2.67
50623   0.30  Ideal     E     SI2   62.1   54.0    401  4.32  4.35  2.69
50625   0.30  Ideal     E     SI2   62.0   54.0    401  4.33  4.35  2.69
```


### DataFrame transformation

#### `mutate()`

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

#### `transmute()`

The `transmute()` function is a combination of a mutate and a selection of the
created variables.

```python
diamonds >> transmute(x_plus_y=X.x + X.y, y_div_z=(X.y / X.z)) >> head(3)

    y_div_z  x_plus_y
0  1.637860      7.93
1  1.662338      7.73
2  1.761905      8.12
```

### Grouping

#### `groupby()` and `ungroup()`

DataFrames are grouped along variables using the `groupby()` function and
ungrouped with the `ungroup()` function. Functions chained after grouping a
DataFrame are applied by group until returning or ungrouping. Hierarchical/multiindexing is automatically removed.

In the example below, the `lead()` and `lag()` functions are dfply convenience
wrappers around the pandas `.shift()` Series method.


```python
(diamonds >> groupby(X.cut) >>
 mutate(price_lead=lead(X.price), price_lag=lag(X.price)) >>
 head(2) >> select(X.cut, X.price, X.price_lead))

          cut  price  price_lead  price_lag
8        Fair    337         NaN     2757.0
91       Fair   2757       337.0     2759.0
2        Good    327         NaN      335.0
4        Good    335       327.0      339.0
0       Ideal    326         NaN      340.0
11      Ideal    340       326.0      344.0
1     Premium    326         NaN      334.0
3     Premium    334       326.0      342.0
5   Very Good    336         NaN      336.0
6   Very Good    336       336.0      337.0
```


### Reshaping

#### `arrange()`

Sorting is done by the `arrange()` function, which wraps around the pandas
`.sort_values()` DataFrame method. Arguments and keyword arguments are passed
through to that function (arguments are also currently flattened like in the select
functions).

```python
diamonds >> arrange(X.table, ascending=False) >> head(5)

       carat   cut color clarity  depth  table  price     x     y     z
24932   2.01  Fair     F     SI1   58.6   95.0  13387  8.32  8.31  4.87
50773   0.81  Fair     F     SI2   68.8   79.0   2301  5.26  5.20  3.58
51342   0.79  Fair     G     SI1   65.3   76.0   2362  5.52  5.13  3.35
52860   0.50  Fair     E     VS2   79.0   73.0   2579  5.21  5.18  4.09
49375   0.70  Fair     H     VS1   62.0   73.0   2100  5.65  5.54  3.47


(diamonds >> groupby(X.cut) >> arrange(X.price) >>
 head(3) >> ungroup() >> mask(X.carat < 0.23))
       carat        cut color clarity  depth  table  price     x     y     z
28270   0.25       Fair     E     VS1   55.2   64.0    361  4.21  4.23  2.33
13      0.31      Ideal     J     SI2   62.2   54.0    344  4.35  4.37  2.71
5       0.24  Very Good     J    VVS2   62.8   57.0    336  3.94  3.96  2.48
```

#### `rename()`

The `rename()` function will rename columns provided as values to what you set
as the keys in the keyword arguments. You can indicate columns with symbols or
with their labels.

```python
diamonds >> rename(CUT=X.cut, COLOR='color') >> head(2)

   carat      CUT COLOR clarity  depth  table  price     x     y     z
0   0.23    Ideal     E     SI2   61.5   55.0    326  3.95  3.98  2.43
1   0.21  Premium     E     SI1   59.8   61.0    326  3.89  3.84  2.31
```

#### `gather()`

Transforming between "wide" and "long" format is a common pattern in data munging.
The `gather(key, value, *columns)` function melts the specified columns in your
DataFrame into two key:value columns.

```python
diamonds >> gather('variable', 'value', ['price', 'depth','x','y','z']) >> head(5)

   carat      cut color clarity  table variable  value
0   0.23    Ideal     E     SI2   55.0    price  326.0
1   0.21  Premium     E     SI1   61.0    price  326.0
2   0.23     Good     E     VS1   65.0    price  327.0
3   0.29  Premium     I     VS2   58.0    price  334.0
4   0.31     Good     J     SI2   58.0    price  335.0
```

Without any columns specified, your entire DataFrame will be transformed into
two key:value pair columns.

```python
diamonds >> gather('variable', 'value') >> head(5)

  variable value
0    carat  0.23
1    carat  0.21
2    carat  0.23
3    carat  0.29
4    carat  0.31
```

If the `add_id` keyword argument is set to true, an id column is added to the
new elongated DataFrame that acts as a row id from the original wide DataFrame.

```python
elongated = diamonds >> gather('variable', 'value', add_id=True)
elongated >> head(5)

   _ID variable value
0    0    carat  0.23
1    1    carat  0.21
2    2    carat  0.23
3    3    carat  0.29
4    4    carat  0.31
```

#### `spread()`

Likewise, you can transform a "long" DataFrame into a "wide" format with the
`spread(key, values)` function. Converting the previously created elongated
DataFrame for example would be done like so.

```python
widened = elongated >> spread(X.variable, X.value)
widened >> head(5)

    _ID carat clarity color        cut depth price table     x     y     z
0     0  0.23     SI2     E      Ideal  61.5   326    55  3.95  3.98  2.43
1     1  0.21     SI1     E    Premium  59.8   326    61  3.89  3.84  2.31
2    10   0.3     SI1     J       Good    64   339    55  4.25  4.28  2.73
3   100  0.75     SI1     D  Very Good  63.2  2760    56   5.8  5.75  3.65
4  1000  0.75     SI1     D      Ideal  62.3  2898    55  5.83   5.8  3.62
```

In this case the `_ID` column comes in handy since it is necessary to not have
any duplicated identifiers.

If you have a mixed datatype column in your long-format DataFrame then the
default behavior is for the spread columns to be of type object.

```python
widened.dtypes

_ID         int64
carat      object
clarity    object
color      object
cut        object
depth      object
price      object
table      object
x          object
y          object
z          object
dtype: object
```

If you want to try to convert dtypes when spreading, you can set the `convert`
keyword argument in spread to True like so.

```python
widened = elongated >> spread(X.variable, X.value, convert=True)
widened.dtypes

_ID          int64
carat      float64
clarity     object
color       object
cut         object
depth      float64
price        int64
table      float64
x          float64
y          float64
z          float64
dtype: object
```

#### `separate()`

Columns can be split into multiple columns with the
`separate(column, into, sep="[\W_]+", remove=True, convert=False,
extra='drop', fill='right')` function. `separate()` takes a variety of arguments:

- `column`: the column to split.
- `into`: the names of the new columns.
- `sep`: either a regex string or integer positions to split the column on.
- `remove`: boolean indicating whether to remove the original column.
- `convert`: boolean indicating whether the new columns should be converted to
the appropriate type (same as in `spread` above).
- `extra`: either `drop`, where split pieces beyond the specified new columns
are dropped, or `merge`, where the final split piece contains the remainder of
the original column.
- `fill`: either `right`, where `np.nan` values are filled in the right-most
columns for missing pieces, or `left` where `np.nan` values are filled in the
left-most columns.

```python
print d

         a
0    1-a-3
1      1-b
2  1-c-3-4
3    9-d-1
4       10

d >> separate(X.a, ['col1', 'col2'], remove=True, convert=True,
              extra='drop', fill='right')

   col1 col2
0     1    a
1     1    b
2     1    c
3     9    d
4    10  NaN

d >> separate(X.a, ['col1', 'col2'], remove=True, convert=True,
              extra='drop', fill='left')

   col1 col2
0   1.0    a
1   1.0    b
2   1.0    c
3   9.0    d
4   NaN   10

d >> separate(X.a, ['col1', 'col2'], remove=False, convert=True,
              extra='merge', fill='right')

         a  col1   col2
0    1-a-3     1    a-3
1      1-b     1      b
2  1-c-3-4     1  c-3-4
3    9-d-1     9    d-1
4       10    10    NaN

d >> separate(X.a, ['col1', 'col2', 'col3'], sep=[2,4], remove=True, convert=True,
              extra='merge', fill='right')

  col1 col2 col3
0   1-   a-    3
1   1-    b  NaN
2   1-   c-  3-4
3   9-   d-    1
4   10  NaN  NaN
```

#### `unite()`

The `unite(colname, *args, sep='_', remove=True, na_action='maintain')` function
does the inverse of `separate()`, joining columns together by a separator. Any
columns that are not strings will be converted to strings. The arguments for
`unite()` are:

- `colname`: the name of the new joined column.
- `*args`: list of columns to be joined, which can be strings, symbolic, or
integer positions.
- `sep`: the string separator to join the columns with.
- `remove`: boolean indicating whether or not to remove the original columns.
- `na_action`: can be one of `"maintain"` (the default), `"ignore"`, or
`"as_string"`. The default `"maintain"` will make the new column row a `NaN` value
if any of the original column cells at that row contained `NaN`. `"ignore"` will
treat any `NaN` value as an empty string during joining. `"as_string"` will convert
any `NaN` value to the string `"nan"` prior to joining.

```python

print d

a  b      c
0  1  a   True
1  2  b  False
2  3  c    NaN

d >> unite('united', X.a, 'b', 2, remove=False, na_action='maintain')

   a  b      c     united
0  1  a   True   1_a_True
1  2  b  False  2_b_False
2  3  c    NaN        NaN

d >> unite('united', ['a','b','c'], remove=True, na_action='ignore', sep='*')

      united
0   1*a*True
1  2*b*False
2        3*c

d >> unite('united', d.columns, remove=True, na_action='as_string')

      united
0   1_a_True
1  2_b_False
2    3_c_nan
```


### Joining

Currently implemented joins are:

1. `inner_join(other, by='column')`
- `outer_join(other, by='column')` (which works the same as `full_join()`)
- `right_join(other, by='column')`
- `left_join(other, by='column')`
- `semi_join(other, by='column')`
- `anti_join(other, by='column')`

The functionality of the join functions are outlined with the toy example
DataFrames below.

```python
a = pd.DataFrame({
        'x1':['A','B','C'],
        'x2':[1,2,3]
    })
b = pd.DataFrame({
    'x1':['A','B','D'],
    'x3':[True,False,True]
})
```

#### `inner_join()`

`inner_join()` joins on values present in both DataFrames' `by` columns.

```python
a >> inner_join(b, by='x1')

  x1  x2     x3
0  A   1   True
1  B   2  False
```

#### `outer_join()` or `full_join()`

`outer_join` merges DataFrame's together on values present in either frame's
`by` columns.

```python
a >> outer_join(b, by='x1')

  x1   x2     x3
0  A  1.0   True
1  B  2.0  False
2  C  3.0    NaN
3  D  NaN   True
```

#### `left_join()`

`left_join` merges on the values present in the left DataFrame's `by` columns.

```python
a >> left_join(b, by='x1')

  x1  x2     x3
0  A   1   True
1  B   2  False
2  C   3    NaN
```

#### `right_join()`

`right_join` merges on the values present in the right DataFrame's `by` columns.

```python
a >> right_join(b, by='x1')

  x1   x2     x3
0  A  1.0   True
1  B  2.0  False
2  D  NaN   True
```

#### `semi_join()`

`semi_join()` returns all of the rows in the left DataFrame that have a match
in the right DataFrame in the `by` columns.

```python
a >> semi_join(b, by='x1')

  x1  x2
0  A   1
1  B   2
```

#### `anti_join()`

`anti_join()` returns all of the rows in the left DataFrame that do not have a
match in the right DataFrame within the `by` columns.

```python
a >> anti_join(b, by='x1')

  x1  x2
2  C   3
```


### Set operations

The set operation functions filter a DataFrame based on row comparisons with
another DataFrame.

Each of the set operation functions `union()`, `intersect()`, and `set_diff()`
take the same arguments:

- `other`: the DataFrame to compare to
- `index`: a boolean (default `False`) indicating whether to consider the pandas
index during comparison.
- `keep`: string (default `"first"`) to be passed through to `.drop_duplicates()`
controlling how to handle duplicate rows.

With set operations columns are expected to be in the same order in both
DataFrames.

The function examples use the following two toy DataFrames.

```python
a = pd.DataFrame({
        'x1':['A','B','C'],
        'x2':[1,2,3]
    })
c = pd.DataFrame({
      'x1':['B','C','D'],
      'x2':[2,3,4]
})
```

#### `union()`

The `union()` function returns rows that appear in either DataFrame.

```python
a >> union(c)

  x1  x2
0  A   1
1  B   2
2  C   3
2  D   4
```

#### `intersect()`

`intersect()` returns rows that appear in both DataFrames.

```python
a >> intersect(c)

  x1  x2
0  B   2
1  C   3
```


#### `set_diff()`

`set_diff()` returns the rows in the left DataFrame that do not appear in the
right DataFrame.

```python
a >> set_diff(c)

  x1  x2
0  A   1
```


### Binding

dfply comes with convenience wrappers around `pandas.concat()` for joining
DataFrames by rows or by columns.

The toy DataFrames below (`a` and `b`) are the same as the ones used to display
the join functions above.

#### `bind_rows()`

The `bind_rows(other, join='outer', ignore_index=False)` function is an exact
call to `pandas.concat([df, other], join=join, ignore_index=ignore_index, axis=0)`,
joining two DataFrames "vertically".

```python
a >> bind_rows(b, join='inner')

x1
0  A
1  B
2  C
0  A
1  B
2  D

a >> bind_rows(b, join='outer')

  x1   x2     x3
0  A  1.0    NaN
1  B  2.0    NaN
2  C  3.0    NaN
0  A  NaN   True
1  B  NaN  False
2  D  NaN   True
```

Note that `bind_rows()` does not reset the index for you!

#### `bind_cols()`

The `bind_cols(other, join='outer', ignore_index=False)` is likewise just a
call to `pandas.concat([df, other], join=join, ignore_index=ignore_index, axis=1)`,
joining DataFrames "horizontally".

```python
a >> bind_cols(b)

  x1  x2 x1     x3
0  A   1  A   True
1  B   2  B  False
2  C   3  D   True
```

Note that you may well end up with duplicate column labels after binding columns
as can be seen above.


### Summarization

There are two summarization functions in dfply that match dplr: `summarize` and
`summarize_each` (though these functions use the 'z' spelling rather than 's').

#### `summarize()`

`summarize(**kwargs)` takes an arbitrary number of keyword arguments that will
return new columns labeled with the keys that are summary functions of columns
in the original DataFrame.

```python
diamonds >> summarize(price_mean=X.price.mean(), price_std=X.price.std())

    price_mean    price_std
0  3932.799722  3989.439738
```

`summarize()` can of course be used with groupings as well.

```python
diamonds >> groupby('cut') >> summarize(price_mean=X.price.mean(), price_std=X.price.std())

         cut   price_mean    price_std
0       Fair  4358.757764  3560.386612
1       Good  3928.864452  3681.589584
2      Ideal  3457.541970  3808.401172
3    Premium  4584.257704  4349.204961
4  Very Good  3981.759891  3935.862161
```

#### `summarize_each()`

The `summarize_each(function_list, *columns)` is a more general summarization
function. It takes a list of summary functions to apply as its first argument and
then a list of columns to apply the summary functions to. Columns can be specified
with either symbolic, string label, or integer position like in the selection
functions for convenience.

```python
diamonds >> summarize_each([np.mean, np.var], X.price, 'depth')

    price_mean     price_var  depth_mean  depth_var
0  3932.799722  1.591533e+07   61.749405   2.052366
```

`summarize_each()` works with groupings as well.

```python
diamonds >> groupby(X.cut) >> summarize_each([np.mean, np.var], X.price, 4)

         cut   price_mean     price_var  depth_mean  depth_var
0       Fair  4358.757764  1.266848e+07   64.041677  13.266319
1       Good  3928.864452  1.355134e+07   62.365879   4.705224
2      Ideal  3457.541970  1.450325e+07   61.709401   0.516274
3    Premium  4584.257704  1.891421e+07   61.264673   1.342755
4  Very Good  3981.759891  1.548973e+07   61.818275   1.900466
```


## Decorators

Under the hood, dfply functions work using a collection of different decorators.
Each decorator performs a specific operation on the function parameters, and
the variety of dfply function behavior is made possible by this compartmentalization.

### `@pipe`

The primary decorator that makes chaining functions with the `>>` operator
is `@pipe`. For functions to work with the piping syntax they must be decorated
with `@pipe`.

Any function decorated with `@pipe` implicitly receives a single first argument
expected to be a pandas DataFrame. This is the DataFrame being passed through
the pipe. For example, `mutate` and `select` have function specifications
`mutate(df, **kwargs)` and `select(df, *args, **kwargs)`, but when used
do not require the user to insert the DataFrame as an argument.

```python
# the DataFrame is implicitly passed as the first argument
diamonds >> mutate(new_var=X.price + X.depth) >> select(X.new_var)
```

If you create a new function decorated by `@pipe`, the function definition
should contain an initial argument that represents the DataFrame being passed
through the piping operations.

```python
@pipe
def myfunc(df, *args, **kwargs):
  # code
```

### `@group_delegation`

In order to delegate a function across specified groupings (assigned by the
`groupby()` function), decorate the function with the `@group_delegation`
decorator. This decorator will query the DataFrame for assigned groupings and
apply the function to those groups individually.

Groupings are assigned by dfply as an attribute `._grouped_by` to the DataFrame
proceeding through the piped functions. `@group_delegation` checks for the
attribute and applies the function by group if groups exist. Any hierarchical
indexing is removed by the decorator as well.

Decoration by `@group_delegation` should come after (internal) to the `@pipe`
decorator to function as intended.

```python
@pipe
@group_delegation
def myfunc(df, *args, **kwargs):
  # code
```

### `@symbolic_evaluation` and `@symbolic_reference`

Evaluation of the symbolic pandas-ply `X` DataFrame by piped functions is
handled by the `@symbolic_evaluation` function. For example, when calling
`mutate(new_price = X.price * 2.5)` the `X.price` symbolic representation of
the price column in the DataFrame will be evaluated to the actual Series
by the decorator.

`@symbolic_reference` tries to evaluate the _label_ or name of the symbolic object
rather than the actual values. This is particularly useful for the selection
and dropping functions where the index of the columns is desired rather than
the actual values of the column. That being said, `@symbolic_reference` is merely
a convenience; decorating a function with `@symbolic_evaluation` and then
manually extracting the labels of the Series or DataFrame objects within the
decorated function would behave the same.


### `@dfpipe`

Most new or custom functions for dfply will be decorated with the pattern:

```python
@pipe
@group_delegation
@symbolic_evaluation
def myfunc(df, *args, **kwargs):
  # code
```

Because of this, the decorator `@dfpipe` is defined as exactly this combination
of decorators for your convenience. The above decoration pattern for the function
can be simply written as:

```python
@dfpipe
def myfunc(df, *args, **kwargs):
  # code
```

This allows you to easily create new functions that can be chained together
with pipes, respect grouping, and evaluate symbolic DataFrames and Series
correctly.


### Extending and mixing behavior with decorators

One of the primary reasons that the dfply logic was built on these decorators
was to make the package easily extensible. Though decoration
of functions should typically follow a basic order (`@pipe` first, then `@group_delegation`,
etc.), choosing to include or omit certain decorators in the chain allows the behavior of
your functions to be easily customized.

The currently built-in decorators are:

- `@pipe`: controlling piping through `>>` operators.
- `@group_delegation`: controlling the delegation of functions by grouping.
- `@symbolic_evaluation`: evaluating symbolic pandas-ply DataFrames and Series.
- `@symbolic_reference`: evaluating symbolic objects to their labels/names.
- `@dfpipe`: decorator chaining `@pipe`, `@group_delegation`, and `@symbolic_evaluation`.
- `@flatten_arguments`: extracts arguments in lists/tuples to be single arguments
for the decorated function.
- `@join_index_arguments`: joins single and list/array arguments into a single
numpy array (used by row_slice).
- `@column_indices_as_labels`: converts string, integer, and symbolic arguments
referring to columns into string column labels for the decorated function.
- `@column_indices_as_positions`: converts string, integer, and symbolic arguments
referring to columns into integer column positions for the decorated function.
- `@label_selection`: chains together `@pipe`, `@group_delegation`, `@symbolic_reference`,
and `@column_indices_as_labels`.
- `@positional_selection`: chains together `@pipe`, `@group_delegation`, `@symbolic_reference`,
and `@column_indices_as_positions`.

For many examples of these decorators and how they can be used together to achieve
different types of behavior on piped DataFrames, please see the source code!

## Contributing

By all means please feel free to comment or contribute to the package. If you
submit an issue, pull request, or ask for something to be added I will do my
best to respond promptly.

The TODO list (now located in the "Projects" section of the repo) has an
ongoing list of things that still need to be resolved and features to be added.

If you submit a pull request with features or bugfixes, please target the
"develop" branch rather than the "master" branch.


## TODO:

**TODO list has been moved to the "Projects" section of the github repo.**
