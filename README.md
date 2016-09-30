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
extensible. For a more in-depth overview of the decorators and how dfply can be
customized see below.

<!-- START doctoc -->
<!-- END doctoc -->

# Overview and basic usage

> (An ipython notebook showcasing working features of dfply [can be 
found here](https://github.com/kieferk/dfply/blob/master/examples/dfply-example-gallery.ipynb))
