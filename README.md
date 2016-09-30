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

# Overview and basic usage

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
