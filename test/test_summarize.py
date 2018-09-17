import pytest

from dfply import *


##==============================================================================
## summarization test functions
##==============================================================================

def test_summarize():
    p = pd.DataFrame({
        'price_mean':[diamonds.price.mean()],
        'price_std':[diamonds.price.std()]
    })
    assert p.equals(diamonds >> summarize(price_mean=X.price.mean(),
                                          price_std=X.price.std()))

    pcut = pd.DataFrame({
        'cut':['Fair','Good','Ideal','Premium','Very Good']
    })
    pcut['price_mean'] = [diamonds[diamonds.cut == c].price.mean() for c in pcut.cut.values]
    pcut['price_std'] = [diamonds[diamonds.cut == c].price.std() for c in pcut.cut.values]
    assert pcut.equals(diamonds >> group_by('cut') >>
                       summarize(price_mean=X.price.mean(), price_std=X.price.std()))


def test_summarize_with_positional_args():
    with_args = diamonds >> summarize(
        X.price.mean(),
        price_std=X.price.std()
    ) >> rename(
        price_mean=X.unnamed_arg_0
    )

    with_kwargs = diamonds >> summarize(
        price_std=X.price.std(),
        price_mean=X.price.mean()
    )

    # Use `sort_index()` to account for
    # Python versions < 3.6 which do not
    # reliably conserve dictionary insertion order.
    pd.testing.assert_frame_equal(
        with_kwargs.sort_index(axis=1),
        with_args.sort_index(axis=1)
    )


def test_summarize_each():
    to_match = pd.DataFrame({
        'price_mean':[np.mean(diamonds.price)],
        'price_var':[np.var(diamonds.price)],
        'depth_mean':[np.mean(diamonds.depth)],
        'depth_var':[np.var(diamonds.depth)]
    })
    to_match = to_match[['price_mean','price_var','depth_mean','depth_var']]

    test1 = diamonds >> summarize_each([np.mean, np.var], X.price, 4)
    test2 = diamonds >> summarize_each([np.mean, np.var], X.price, 'depth')
    assert to_match.equals(test1)
    assert to_match.equals(test2)

    group = pd.DataFrame({
        'cut':['Fair','Good','Ideal','Premium','Very Good']
    })
    group['price_mean'] = [np.mean(diamonds[diamonds.cut == c].price) for c in group.cut.values]
    group['price_var'] = [np.var(diamonds[diamonds.cut == c].price) for c in group.cut.values]
    group['depth_mean'] = [np.mean(diamonds[diamonds.cut == c].depth) for c in group.cut.values]
    group['depth_var'] = [np.var(diamonds[diamonds.cut == c].depth) for c in group.cut.values]

    group = group[['cut','price_mean','price_var','depth_mean','depth_var']]

    test1 = (diamonds >> group_by(X.cut) >>
             summarize_each([np.mean, np.var], X.price, 4))
    test2 = (diamonds >> group_by('cut') >>
             summarize_each([np.mean, np.var], X.price, 'depth'))

    assert group.equals(test1)
    assert group.equals(test2)
