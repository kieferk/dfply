# dfply

### Version: 0.3.2

> Note: Version 0.3.0 is the first big update in awhile, and changes a lot of
the "base" code. The `pandas-ply` package is no longer being imported. I have coded
my own version of the "symbolic" objects that I was borrowing from `pandas-ply`. Also,
I am no longer supporting Python 2, sorry!

> **In v0.3 `groupby` has been renamed to `group_by` to mirror the `dplyr` function.
If this breaks your legacy code, one possible fix is to have `from dfply.group import group_by as groupby`
in your package imports.**

The `dfply` package makes it possible to do R's `dplyr`-style data manipulation with pipes
in python on pandas DataFrames.

This is an alternative to [`pandas-ply`](https://github.com/coursera/pandas-ply)
and [`dplython`](https://github.com/dodger487/dplython), which both engineer `dplyr`
syntax and functionality in python. There are probably more packages that attempt
to enable `dplyr`-style dataframe manipulation in python, but those are the two I
am aware of.

`dfply` uses a decorator-based architecture for the piping functionality and
to "categorize" the types of data manipulation functions. The goal of this  
architecture is to make `dfply` concise and easily extensible, simply by chaining
together different decorators that each have a distinct effect on the wrapped
function. There is a more in-depth overview of the decorators and how `dfply` can be
customized below.

`dfply` is intended to mimic the functionality of `dplyr`. The syntax
is the same for the most part, but will vary in some cases as Python is a
considerably different programming language than R.

A good amount of the core functionality of `dplyr` is complete, and the remainder is
actively being added in. Going forward I hope functionality that is not
directly part of `dplyr` to be incorporated into `dfply` as well. This is not
intended to be an absolute mimic of `dplyr`, but instead a port of the _ease,
convenience and readability_ the `dplyr` package provides for data manipulation
tasks.

**Expect frequent updates to the package version as features are added and
any bugs are fixed.**

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Overview of functions](#overview-of-functions)
  - [The `>>` and `>>=` pipe operators](#the--and--pipe-operators)
  - [The `X` DataFrame symbol](#the-x-dataframe-symbol)
  - [Selecting and dropping](#selecting-and-dropping)
    - [`select()` and `drop()` functions](#select-and-drop-functions)
    - [Selection using the inversion `~` operator on symbolic columns](#selection-using-the-inversion--operator-on-symbolic-columns)
    - [Selection filter functions](#selection-filter-functions)
  - [Subsetting and filtering](#subsetting-and-filtering)
    - [`row_slice()`](#row_slice)
    - [`sample()`](#sample)
    - [`distinct()`](#distinct)
    - [`mask()`](#mask)
  - [DataFrame transformation](#dataframe-transformation)
    - [`mutate()`](#mutate)
    - [`transmute()`](#transmute)
  - [Grouping](#grouping)
    - [`group_by()` and `ungroup()`](#group_by-and-ungroup)
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
- [Embedded column functions](#embedded-column-functions)
  - [Window functions](#window-functions)
    - [`lead()` and `lag()`](#lead-and-lag)
    - [`between()`](#between)
    - [`dense_rank()`](#dense_rank)
    - [`min_rank()`](#min_rank)
    - [`cumsum()`](#cumsum)
    - [`cummean()`](#cummean)
    - [`cummax()`](#cummax)
    - [`cummin()`](#cummin)
    - [`cumprod()`](#cumprod)
  - [Summary functions](#summary-functions)
    - [`mean()`](#mean)
    - [`first()`](#first)
    - [`last()`](#last)
    - [`nth()`](#nth)
    - [`n()`](#n)
    - [`n_distinct()`](#n_distinct)
    - [`IQR()`](#iqr)
    - [`colmin()`](#colmin)
    - [`colmax()`](#colmax)
    - [`median()`](#median)
    - [`var()`](#var)
    - [`sd()`](#sd)
- [Extending `dfply` with custom functions](#extending-dfply-with-custom-functions)
  - [Case 1: A custom "pipe" function with `@dfpipe`](#case-1-a-custom-pipe-function-with-dfpipe)
  - [Case 2: A function that works with symbolic objects using `@make_symbolic`](#case-2-a-function-that-works-with-symbolic-objects-using-make_symbolic)
    - [Without symbolic arguments, `@make_symbolic` functions work like normal functions!](#without-symbolic-arguments-make_symbolic-functions-work-like-normal-functions)
- [Advanced: understanding base `dfply` decorators](#advanced-understanding-base-dfply-decorators)
  - [The `Intention` class](#the-intention-class)
  - [`@pipe`](#pipe)
  - [`@group_delegation`](#group_delegation)
  - [`@symbolic_evaluation`](#symbolic_evaluation)
    - [Controlling `@symbolic_evaluation` with the `eval_symbols` argument](#controlling-symbolic_evaluation-with-the-eval_symbols-argument)
  - [`@dfpipe`](#dfpipe)
  - [`@make_symbolic`](#make_symbolic)
- [Contributing](#contributing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview of functions

### The `>>` and `>>=` pipe operators

dfply works directly on pandas DataFrames, chaining operations on the data with
the `>>` operator, or alternatively starting with `>>=` for inplace operations.

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

Inplace operations are done with the first pipe as `>>=` and subsequent pipes
as `>>`.

```python
diamonds >>= head(10) >> tail(3)

diamonds

   carat        cut color clarity  depth  table  price     x     y     z
7   0.26  Very Good     H     SI1   61.9   55.0    337  4.07  4.11  2.53
8   0.22       Fair     E     VS2   65.1   61.0    337  3.87  3.78  2.49
9   0.23  Very Good     H     VS1   59.4   61.0    338  4.00  4.05  2.39
```

When using the inplace pipe, the DataFrame is not required on the left hand
side of the `>>=` pipe and the DataFrame variable is overwritten with the
output of the operations.


### The `X` DataFrame symbol

The DataFrame as it is passed through the piping operations is represented
by the symbol `X`. It records the actions you want to take (represented by
the `Intention` class), but does not evaluate them until the appropriate time.
Operations on the DataFrame are deferred. Selecting
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

#### `select()` and `drop()` functions

There are two functions for selection, inverse of each other: `select` and
`drop`. The `select` and `drop` functions accept string labels, integer positions,
and/or symbolically represented column names (`X.column`). They also accept symbolic "selection
filter" functions, which will be covered shortly.

The example below selects "cut", "price", "x", and "y" from the diamonds dataset.

```python
diamonds >> select(1, X.price, ['x', 'y']) >> head(2)

       cut  price     x     y
0    Ideal    326  3.95  3.98
1  Premium    326  3.89  3.84
```

If you were instead to use `drop`, you would get back all columns besides those specified.

```python
diamonds >> drop(1, X.price, ['x', 'y']) >> head(2)

   carat color clarity  depth  table     z
0   0.23     E     SI2   61.5   55.0  2.43
1   0.21     E     SI1   59.8   61.0  2.31
```


#### Selection using the inversion `~` operator on symbolic columns

One particularly nice thing about `dplyr`'s selection functions is that you can
drop columns inside of a select statement by putting a subtraction sign in front,
like so: `... %>% select(-col)`. The same can be done in `dfply`, but instead of
the subtraction operator you use the tilde `~`.

For example, let's say I wanted to select any column _except_ carat, color, and
clarity in my dataframe. One way to do this is to specify those for removal using
the `~` operator like so:


```python
diamonds >> select(~X.carat, ~X.color, ~X.clarity) >> head(2)

       cut  depth  table  price     x     y     z
0    Ideal   61.5   55.0    326  3.95  3.98  2.43
1  Premium   59.8   61.0    326  3.89  3.84  2.31
```

Note that if you are going to use the inversion operator, you _must_ place it
prior to the symbolic `X` (or a symbolic such as a selection filter function, covered
next). For example, using the inversion operator on a list of columns will
result in an error:

```python
diamonds >> select(~[X.carat, X.color, X.clarity]) >> head(2)

TypeError: bad operand type for unary ~: 'list'
```


#### Selection filter functions

The vanilla `select` and `drop` functions are useful, but there are a variety of
selection functions inspired by `dplyr` available to make selecting and dropping
columns a breeze. These functions are intended to be put inside of the `select` and
`drop` functions, and can be paired with the `~` inverter.

First, a quick rundown of the available functions:
- `starts_with(prefix)`: find columns that start with a string prefix.
- `ends_with(suffix)`: find columns that end with a string suffix.
- `contains(substr)`: find columns that contain a substring in their name.
- `everything()`: all columns.
- `columns_between(start_col, end_col, inclusive=True)`: find columns between a specified start and end column.
The `inclusive` boolean keyword argument indicates whether the end column should be included or not.
- `columns_to(end_col, inclusive=True)`: get columns up to a specified end column. The `inclusive`
argument indicates whether the ending column should be included or not.
- `columns_from(start_col)`: get the columns starting at a specified column.

The selection filter functions are best explained by example. Let's say I wanted to
select only the columns that started with a "c":

```python
diamonds >> select(starts_with('c')) >> head(2)

   carat      cut color clarity
0   0.23    Ideal     E     SI2
1   0.21  Premium     E     SI1
```

The selection filter functions are instances of the class `Intention`, just like the
`X` placeholder, and so I can also use the inversion operator with them. For example,
I can alternatively select the columns that do not start with "c":

```python
diamonds >> select(~starts_with('c')) >> head(2)

   depth  table  price     x     y     z
0   61.5   55.0    326  3.95  3.98  2.43
1   59.8   61.0    326  3.89  3.84  2.31
```

They work the same inside the `drop` function, but with the intention of removal.
I could, for example, use the `columns_from` selection filter to drop all columns
from "price" onwards:

```python
diamonds >> drop(columns_from(X.price)) >> head(2)

   carat      cut color clarity  depth  table
0   0.23    Ideal     E     SI2   61.5   55.0
1   0.21  Premium     E     SI1   59.8   61.0
```

As the example above shows, you can use symbolic column names inside of the
selection filter function! You can also mix together selection filters and standard
selections inside of the same `select` or `drop` command.

For my next trick, I will select the first two columns, the last two columns, and
the "depth" column using a mixture of selection techniques:

```python
diamonds >> select(columns_to(1, inclusive=True), 'depth', columns_from(-2)) >> head(2)

   carat      cut  depth     y     z
0   0.23    Ideal   61.5  3.98  2.43
1   0.21  Premium   59.8  3.84  2.31
```


### Subsetting and filtering

#### `row_slice()`

Slices of rows can be selected with the `row_slice()` function. You can pass
single integer indices or a list of indices to select rows as with. This is
going to be the same as using pandas' `.iloc`.

```python
diamonds >> row_slice([10,15])

    carat      cut color clarity  depth  table  price     x     y     z
10   0.30     Good     J     SI1   64.0   55.0    339  4.25  4.28  2.73
15   0.32  Premium     E      I1   60.9   58.0    345  4.38  4.42  2.68
```

Note that this can also be used with the `group_by` function, and will operate
like a call to `.iloc` on each group. The `group_by` pipe function is
covered later, but it essentially works the same as pandas `.groupby` (with a
few subtle differences).

```python
diamonds >> group_by('cut') >> row_slice(5)

     carat        cut color clarity  depth  table  price     x     y     z
128   0.91       Fair     H     SI2   64.4   57.0   2763  6.11  6.09  3.93
20    0.30       Good     I     SI2   63.3   56.0    351  4.26  4.30  2.71
40    0.33      Ideal     I     SI2   61.2   56.0    403  4.49  4.50  2.75
26    0.24    Premium     I     VS1   62.5   57.0    355  3.97  3.94  2.47
21    0.23  Very Good     E     VS2   63.8   55.0    352  3.85  3.92  2.48
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

Alternatively, `mask()` can also be called using the alias `filter_by()`:

```python
diamonds >> filter_by(X.cut == 'Ideal', X.color == 'E', X.table < 55, X.price < 500)

       carat    cut color clarity  depth  table  price     x     y     z
26683   0.33  Ideal     E     SI2   62.2   54.0    427  4.44  4.46  2.77
32297   0.34  Ideal     E     SI2   62.4   54.0    454  4.49  4.52  2.81
40928   0.30  Ideal     E     SI1   61.6   54.0    499  4.32  4.35  2.67
50623   0.30  Ideal     E     SI2   62.1   54.0    401  4.32  4.35  2.69
50625   0.30  Ideal     E     SI2   62.0   54.0    401  4.33  4.35  2.69
```

#### `pull()`

`pull` simply retrieves a column and returns it as a pandas series, in case you only care about one particular column at the end of your pipeline. Columns can be specified either by their name (string) or an integer.

The default returns the last column (on the assumption that's the column you've created most recently).

Example:

```python
(diamonds
 >> filter_by(X.cut == 'Ideal', X.color == 'E', X.table < 55, X.price < 500)
 >> pull('carat'))

 26683    0.33
 32297    0.34
 40928    0.30
 50623    0.30
 50625    0.30
 Name: carat, dtype: float64
```


### DataFrame transformation

#### `mutate()`

New variables can be created with the `mutate()` function (named that way to match
`dplyr`).

```python
diamonds >> mutate(x_plus_y=X.x + X.y) >> select(columns_from('x')) >> head(3)

      x     y     z  x_plus_y
0  3.95  3.98  2.43      7.93
1  3.89  3.84  2.31      7.73
2  4.05  4.07  2.31      8.12
```

Multiple variables can be created in a single call.

```python
diamonds >> mutate(x_plus_y=X.x + X.y, y_div_z=(X.y / X.z)) >> select(columns_from('x')) >> head(3)

      x     y     z  x_plus_y   y_div_z
0  3.95  3.98  2.43      7.93  1.637860
1  3.89  3.84  2.31      7.73  1.662338
2  4.05  4.07  2.31      8.12  1.761905
```

> Note: In Python the new variables created with mutate may not be guaranteed
to be created in the same order that they are input into the function call, though
this may have been changed in Python 3...


#### `transmute()`

The `transmute()` function is a combination of a mutate and a selection of the
created variables.

```python
diamonds >> transmute(x_plus_y=X.x + X.y, y_div_z=(X.y / X.z)) >> head(3)

   x_plus_y   y_div_z
0      7.93  1.637860
1      7.73  1.662338
2      8.12  1.761905
```


### Grouping

#### `group_by()` and `ungroup()`

DataFrames are grouped along variables using the `group_by()` function and
ungrouped with the `ungroup()` function. Functions chained after grouping a
DataFrame are applied by group until returning or ungrouping. Hierarchical/multiindexing
is automatically removed.

> Note: In the example below, the `lead()` and `lag()` functions are dfply convenience
wrappers around the pandas `.shift()` Series method.


```python
(diamonds >> group_by(X.cut) >>
 mutate(price_lead=lead(X.price), price_lag=lag(X.price)) >>
 head(2) >> select(X.cut, X.price, X.price_lead, X.price_lag))

          cut  price  price_lead  price_lag
8        Fair    337      2757.0        NaN
91       Fair   2757      2759.0      337.0
2        Good    327       335.0        NaN
4        Good    335       339.0      327.0
0       Ideal    326       340.0        NaN
11      Ideal    340       344.0      326.0
1     Premium    326       334.0        NaN
3     Premium    334       342.0      326.0
5   Very Good    336       336.0        NaN
6   Very Good    336       337.0      336.0
```


### Reshaping

#### `arrange()`

Sorting is done by the `arrange()` function, which wraps around the pandas
`.sort_values()` DataFrame method. Arguments and keyword arguments are passed
through to that function.

```python
diamonds >> arrange(X.table, ascending=False) >> head(5)

       carat   cut color clarity  depth  table  price     x     y     z
24932   2.01  Fair     F     SI1   58.6   95.0  13387  8.32  8.31  4.87
50773   0.81  Fair     F     SI2   68.8   79.0   2301  5.26  5.20  3.58
51342   0.79  Fair     G     SI1   65.3   76.0   2362  5.52  5.13  3.35
52860   0.50  Fair     E     VS2   79.0   73.0   2579  5.21  5.18  4.09
49375   0.70  Fair     H     VS1   62.0   73.0   2100  5.65  5.54  3.47


(diamonds >> group_by(X.cut) >> arrange(X.price) >>
 head(3) >> ungroup() >> mask(X.carat < 0.23))

    carat      cut color clarity  depth  table  price     x     y     z
8    0.22     Fair     E     VS2   65.1   61.0    337  3.87  3.78  2.49
1    0.21  Premium     E     SI1   59.8   61.0    326  3.89  3.84  2.31
12   0.22  Premium     F     SI1   60.4   61.0    342  3.88  3.84  2.33
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

`dfply` comes with convenience wrappers around `pandas.concat()` for joining
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

There are two summarization functions in `dfply` that match `dplr`: `summarize` and
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
diamonds >> group_by('cut') >> summarize(price_mean=X.price.mean(), price_std=X.price.std())

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
diamonds >> group_by(X.cut) >> summarize_each([np.mean, np.var], X.price, 4)

         cut   price_mean     price_var  depth_mean  depth_var
0       Fair  4358.757764  1.266848e+07   64.041677  13.266319
1       Good  3928.864452  1.355134e+07   62.365879   4.705224
2      Ideal  3457.541970  1.450325e+07   61.709401   0.516274
3    Premium  4584.257704  1.891421e+07   61.264673   1.342755
4  Very Good  3981.759891  1.548973e+07   61.818275   1.900466
```


## Embedded column functions

**UNDER CONSTRUCTION: documentation not complete.**

Like `dplyr`, the `dfply` package provides functions to perform various operations
on pandas Series. These are typically window functions and summarization
functions, and wrap symbolic arguments in function calls.


### Window functions

Window functions perform operations on vectors of values that return a vector
of the same length.

#### `lead()` and `lag()`

The `lead(series, n)` function pushes values in a vector upward, adding `NaN`
values in the end positions. Likewise, the `lag(series, n)` function
pushes values downward, inserting `NaN` values in the initial positions. Both
are calls to pandas `Series.shift()` function under the hood.

```python
(diamonds >> mutate(price_lead=lead(X.price, 2), price_lag=lag(X.price, 2)) >>
            select(X.price, -2, -1) >>
            head(6))

    price  price_lag  price_lead
 0    326        NaN       327.0
 1    326        NaN       334.0
 2    327      326.0       335.0
 3    334      326.0       336.0
 4    335      327.0       336.0
 5    336      334.0       337.0
```

#### `between()`

The `between(series, a, b, inclusive=False)` function checks to see if values are
between two given bookend values.

```python
diamonds >> select(X.price) >> mutate(price_btwn=between(X.price, 330, 340)) >> head(6)

   price price_btwn
0    326      False
1    326      False
2    327      False
3    334       True
4    335       True
5    336       True
```

#### `dense_rank()`

The `dense_rank(series, ascending=True)` function is a wrapper around the `scipy`
function for calculating dense rank.

```python
diamonds >> select(X.price) >> mutate(price_drank=dense_rank(X.price)) >> head(6)

   price  price_drank
0    326          1.0
1    326          1.0
2    327          2.0
3    334          3.0
4    335          4.0
5    336          5.0
```

#### `min_rank()`

Likewise, `min_rank(series, ascending=True)` is a wrapper around the `scipy` ranking
function with min rank specified.

```python
diamonds >> select(X.price) >> mutate(price_mrank=min_rank(X.price)) >> head(6)

price  price_mrank
0    326          1.0
1    326          1.0
2    327          3.0
3    334          4.0
4    335          5.0
5    336          6.0
```

#### `cumsum()`

The `cumsum(series)` function calculates a cumulative sum of a column.

```python
diamonds >> select(X.price) >> mutate(price_cumsum=cumsum(X.price)) >> head(6)

   price  price_cumsum
0    326           326
1    326           652
2    327           979
3    334          1313
4    335          1648
5    336          1984
```

#### `cummean()`

`cummean(series)`

```python
diamonds >> select(X.price) >> mutate(price_cummean=cummean(X.price)) >> head(6)

   price  price_cummean
0    326     326.000000
1    326     326.000000
2    327     326.333333
3    334     328.250000
4    335     329.600000
5    336     330.666667
```

#### `cummax()`

`cummax(series)`

```python
diamonds >> select(X.price) >> mutate(price_cummax=cummax(X.price)) >> head(6)

   price  price_cummax
0    326         326.0
1    326         326.0
2    327         327.0
3    334         334.0
4    335         335.0
5    336         336.0
```

#### `cummin()`

`cummin(series)`

```python
diamonds >> select(X.price) >> mutate(price_cummin=cummin(X.price)) >> head(6)

   price  price_cummin
0    326         326.0
1    326         326.0
2    327         326.0
3    334         326.0
4    335         326.0
5    336         326.0
```

#### `cumprod()`

`cumprod(series)`

```python
diamonds >> select(X.price) >> mutate(price_cumprod=cumprod(X.price)) >> head(6)

   price     price_cumprod
0    326               326
1    326            106276
2    327          34752252
3    334       11607252168
4    335     3888429476280
5    336  1306512304030080
```


### Summary functions

#### `mean()`

`mean(series)`

```python
diamonds >> groupby(X.cut) >> summarize(price_mean=mean(X.price))

         cut   price_mean
0       Fair  4358.757764
1       Good  3928.864452
2      Ideal  3457.541970
3    Premium  4584.257704
4  Very Good  3981.759891
```

#### `first()`

`first(series, order_by=None)`

```python
diamonds >> groupby(X.cut) >> summarize(price_first=first(X.price))

         cut  price_first
0       Fair          337
1       Good          327
2      Ideal          326
3    Premium          326
4  Very Good          336
```

#### `last()`

`last(series, order_by=None)`

```python
diamonds >> groupby(X.cut) >> summarize(price_last=last(X.price))

         cut  price_last
0       Fair        2747
1       Good        2757
2      Ideal        2757
3    Premium        2757
4  Very Good        2757
```

#### `nth()`

`nth(series, n, order_by=None)`

```python
diamonds >> groupby(X.cut) >> summarize(price_penultimate=nth(X.price, -2))

         cut  price_penultimate
0       Fair               2745
1       Good               2756
2      Ideal               2757
3    Premium               2757
4  Very Good               2757
```

#### `n()`

`n(series)`

```python
diamonds >> groupby(X.cut) >> summarize(price_n=n(X.price))

         cut  price_n
0       Fair     1610
1       Good     4906
2      Ideal    21551
3    Premium    13791
4  Very Good    12082
```

#### `n_distinct()`

`n_distinct(series)`

```python
diamonds >> groupby(X.cut) >> summarize(price_ndistinct=n_distinct(X.price))

         cut  price_ndistinct
0       Fair             1267
1       Good             3086
2      Ideal             7281
3    Premium             6014
4  Very Good             5840
```

#### `IQR()`

`IQR(series)`

```python
diamonds >> groupby(X.cut) >> summarize(price_iqr=IQR(X.price))

         cut  price_iqr
0       Fair    3155.25
1       Good    3883.00
2      Ideal    3800.50
3    Premium    5250.00
4  Very Good    4460.75
```

#### `colmin()`

`colmin(series)`

```python
diamonds >> groupby(X.cut) >> summarize(price_min=colmin(X.price))

         cut  price_min
0       Fair        337
1       Good        327
2      Ideal        326
3    Premium        326
4  Very Good        336
```

#### `colmax()`

`colmax(series)`

```python
diamonds >> groupby(X.cut) >> summarize(price_max=colmax(X.price))

         cut  price_max
0       Fair      18574
1       Good      18788
2      Ideal      18806
3    Premium      18823
4  Very Good      18818
```

#### `median()`

`median(series)`

```python
diamonds >> groupby(X.cut) >> summarize(price_median=median(X.price))

         cut  price_median
0       Fair        3282.0
1       Good        3050.5
2      Ideal        1810.0
3    Premium        3185.0
4  Very Good        2648.0
```

#### `var()`

`var(series)`

```python
diamonds >> groupby(X.cut) >> summarize(price_var=var(X.price))

         cut     price_var
0       Fair  1.267635e+07
1       Good  1.355410e+07
2      Ideal  1.450392e+07
3    Premium  1.891558e+07
4  Very Good  1.549101e+07
```

#### `sd()`

`sd(series)`

```python
diamonds >> groupby(X.cut) >> summarize(price_sd=sd(X.price))

         cut     price_sd
0       Fair  3560.386612
1       Good  3681.589584
2      Ideal  3808.401172
3    Premium  4349.204961
4  Very Good  3935.862161
```


## Extending `dfply` with custom functions

There are a lot of built-in functions, but you are almost certainly going to
reach a point where you want to use some of your own functions with the `dfply`
piping syntax. Luckily, `dfply` comes with two handy decorators to make this
as easy as possible.

> **For a more detailed walkthrough of these two cases, see the [
basics-extending-functionality.ipynb](./examples/basics-extending-functionality.ipynb)
jupyter notebook in the examples folder.**


### Case 1: A custom "pipe" function with `@dfpipe`

You might want to make a custom function that can be a piece of the pipe chain.
For example, say we wanted to write a `dfply` wrapper around a simplified
version of `pd.crosstab`. For the most part, you'll only need to do two things
to make this work:
1. Make sure that your function's first argument will be the dataframe passed in
implicitly by the pipe.
2. Decorate the function with the `@dfpipe` decorator.

Here is an example of the `dfply`-enabled crosstab function:

```python
@dfpipe
def crosstab(df, index, columns):
    return pd.crosstab(index, columns)
```

Normally you could use `pd.crosstab` like so:

```python
pd.crosstab(diamonds.cut, diamonds.color)

color         D     E     F     G     H     I    J
cut                                               
Fair        163   224   312   314   303   175  119
Good        662   933   909   871   702   522  307
Ideal      2834  3903  3826  4884  3115  2093  896
Premium    1603  2337  2331  2924  2360  1428  808
Very Good  1513  2400  2164  2299  1824  1204  678
```

The same result can be achieved now with the custom function in pipe syntax:

```python
diamonds >> crosstab(X.cut, X.color)

color         D     E     F     G     H     I    J
cut                                               
Fair        163   224   312   314   303   175  119
Good        662   933   909   871   702   522  307
Ideal      2834  3903  3826  4884  3115  2093  896
Premium    1603  2337  2331  2924  2360  1428  808
Very Good  1513  2400  2164  2299  1824  1204  678
```


### Case 2: A function that works with symbolic objects using `@make_symbolic`

Many tasks are simpler and do not require the capacity to work as a pipe function. The dfply window functions are the common examples of this: functions that take a Series (or symbolic Series) and return a modified version.


Let's say we had a dataframe with dates represented by strings that we wanted to convert to pandas datetime objects using the pd.to_datetime function. Below is a tiny example dataframe with this issue.

```python
sales = pd.DataFrame(dict(date=['7/10/17','7/11/17','7/12/17','7/13/17','7/14/17'],
                          sales=[1220, 1592, 908, 1102, 1395]))

sales

      date  sales
0  7/10/17   1220
1  7/11/17   1592
2  7/12/17    908
3  7/13/17   1102
4  7/14/17   1395
```

Using the `pd.to_datetime` function inside of a call to mutate will unfortunately
break:

```python
sales >> mutate(pd_date=pd.to_datetime(X.date, infer_datetime_format=True))

...

TypeError: __index__ returned non-int (type Intention)
```

`dfply` functions are special in that they "know" to delay their evaluation until
the data is at that point in the chain. `pd.to_datetime` is not such a function,
and will immediately try to evaluate `X.date`. With a symbolic `Intention` argument
passed in (`X` is an `Intention` object), the function will fail.


Instead, we will need to make a wrapper around `pd.to_datetime`
that can handle these symbolic arguments and delay evaluation until the right time.

It's quite simple: all you need to do is decorate a function with the @make_symbolic decorator:

```python
@make_symbolic
def to_datetime(series, infer_datetime_format=True):
    return pd.to_datetime(series, infer_datetime_format=infer_datetime_format)
```

Now the function can be used with symbolic arguments:

```python
sales >> mutate(pd_date=to_datetime(X.date))

      date  sales    pd_date
0  7/10/17   1220 2017-07-10
1  7/11/17   1592 2017-07-11
2  7/12/17    908 2017-07-12
3  7/13/17   1102 2017-07-13
4  7/14/17   1395 2017-07-14
```

#### Without symbolic arguments, `@make_symbolic` functions work like normal functions!

A particularly nice thing about functions decorated with `@make_symbolic` is that
they will operate normally if passed arguments that are not `Intention` symbolic
objects.

For example, you can pass in the series itself and it will return the new
series of converted dates:

```python
to_datetime(sales.date)

0   2017-07-10
1   2017-07-11
2   2017-07-12
3   2017-07-13
4   2017-07-14
Name: date, dtype: datetime64[ns]
```


## Advanced: understanding base `dfply` decorators

Under the hood, `dfply` functions work using a collection of different decorators and
special classes. Below the most important ones are detailed. Understanding these
are important if you are planning on making big additions or changes to the code.


### The `Intention` class

Python is not a lazily-evaluated language. Typically, something like this
would not work:

```python
diamonds >> select(X.carat) >> head(2)
```

The `X` is supposed to represent the current state of the data through the
piping operator chain, and `X.carat` indicates "select the carat column from
the current data at this point in the chain". But Python will try to evaluate
what `X` is, then what `X.carat` is, then what `select(X.carat)` is, all before
the diamonds dataset ever gets evaluated.

The solution to this is to delay the evaluation until the appropriate time. I will
not get into the granular details here (but feel free to check it out for yourself
in `base.py`). The gist is that things to be delayed are represented by a
special `Intention` class that "waits" until it is time to evaluate the stored
commands with a given dataframe. This is the core of how `dplyr` data manipulation
syntax is made possible in `dfply`.

(Thanks to the creators of the `dplython` and `pandas-ply` for trailblazing a lot
of this before I made this package.)


### `@pipe`

The primary decorator that enables chaining functions with the `>>` operator
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
`group_by()` function), decorate the function with the `@group_delegation`
decorator. This decorator will query the DataFrame for assigned groupings and
apply the function to those groups individually.

Groupings are assigned by `dfply` as an attribute `._grouped_by` to the DataFrame
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

### `@symbolic_evaluation`

Evaluation of any `Intention`-class symbolic object (such as `X`) is
handled by the `@symbolic_evaluation` function. For example, when calling
`mutate(new_price = X.price * 2.5)` the `X.price` symbolic representation of
the price column in the DataFrame will be evaluated to the actual Series
by this decorator.

The `@symbolic_evaluation` decorator can have functionality modified by
optional keyword arguments:


#### Controlling `@symbolic_evaluation` with the `eval_symbols` argument

```python
@symbolic_evaluation(eval_symbols=False)
def my_function(df, arg1, arg2):
    ...
```

If the `eval_symbols` argument is `True`, all symbolics will be evaluated
with the passed-in dataframe. If `False` or `None`, there will be no attempt
to evaluate symbolics.

A list can also be passed in. The list can contain a mix of positional integers
and string keywords, which reference positional arguments and keyworded arguments
respectively. This targets which arguments or keyword arguments to try and
evaluate specifically:


```python
# This indicates that arg1, arg2, and kw1 should be targeted for symbolic
# evaluation, but not the other arguments.
# Note that positional indexes reference arguments AFTER the passed-in dataframe.
# For example, 0 refers to arg1, not df.
@symbolic_evaluation(eval_symbols=[0,1,'kw1'])
def my_function(df, arg1, arg2, arg3, kw1=True, kw2=False):
    ...
```

In reality, you are unlikely to need this behavior unless you really want to
prevent `dfply` from trying to evaluate symbolic arguments. Remember that if
an argument is not symbolic it will be evaluated as normal, so there shouldn't
be much harm leaving it at default other than a little bit of computational overhead.


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


### `@make_symbolic`

Sometimes, like in the window and summary functions that operate on series,
it is necessary to defer the evaluation of a function. For example, in the
code below:

```python
diamonds >> summarize(price_third=nth(X.price, 3))
```

The `nth()` function would typically be evaluated before `summarize()` and the
symbolic argument would not be evaluated at the right time.

The `@make_symbolic` decorator can be placed above functions to convert them
into symbolic functions that will wait to evaluate. Again, this is used
primarily for functions that are embedded inside the function call within
the piping syntax.

The `nth()` code, for example, is below:


```python
@make_symbolic
def nth(series, n, order_by=None):
    if order_by is not None:
        series = order_series_by(series, order_by)
    try:
        return series.iloc[n]
    except:
        return np.nan
```

Functions you write that you want to be able to embed as an argument
can use the `@make_symbolic` to wait until they have access to the DataFrame
to evaluate.



## Contributing

By all means please feel free to comment or contribute to the package. The more
people adding code the better. If you submit an issue, pull request, or ask for
something to be added I will do my best to respond promptly.

The TODO list (now located in the "Projects" section of the repo) has an
ongoing list of things that still need to be resolved and features to be added.

If you submit a pull request with features or bugfixes, please target the
"develop" branch rather than the "master" branch.
