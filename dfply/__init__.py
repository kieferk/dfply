import pandas as pd
import numpy as np
import warnings
from functools import partial, wraps

from .base import (
    contextualize,
    flatten,
    make_symbolic,
    Intention,
    X,
    pipe,
    IntentionEvaluator,
    symbolic_evaluation,
    group_delegation,
    dfpipe
)
from .group import (
    group_by,
    ungroup
)
from .join import (
    get_join_parameters,
    inner_join,
    full_join,
    outer_join,
    left_join,
    right_join,
    semi_join,
    anti_join,
    bind_rows,
    bind_cols
)
from .reshape import (
    arrange,
    rename,
    gather,
    convert_type,
    spread,
    separate,
    unite
)
from .select import (
    selection_context,
    selection_filter,
    resolve_selection,
    select,
    drop,
    select_if,
    drop_if,
    starts_with,
    ends_with,
    contains,
    matches,
    everything,
    num_range,
    one_of,
    columns_between,
    columns_from,
    columns_to
)
from .set_ops import (
    validate_set_ops,
    union,
    intersect,
    set_diff
)
from .subset import (
    head,
    tail,
    sample,
    distinct,
    row_slice,
    mask,
    top_n,
    pull
)
from .summarize import (
    summarize,
    summarize_each
)
from .transform import (
    mutate,
    mutate_if,
    transmute
)
from .data import diamonds
from .summary_functions import (
    mean,
    first,
    last,
    nth,
    n,
    n_distinct,
    IQR,
    colmin,
    colmax,
    median,
    var,
    sd
)
from .window_functions import (
    lead,
    lag,
    between,
    dense_rank,
    min_rank,
    cumsum,
    cummean,
    cummax,
    cummin,
    cumprod,
    cumany,
    cumall,
    percent_rank,
    row_number
)
from .vector import (
    order_series_by,
    desc,
    coalesce,
    case_when,
    if_else,
    na_if
)

for verb in dir():
    if 'ize' in verb:
        exec(verb.replace('ize', 'ise') + '=' + verb)
