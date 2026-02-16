import pytest
import pandas as pd

from unidad1_project import (TransformerMissing,
    TransformerMissingThreshold,
    TransformerNormalizeStrings,
    TransformerImputeMissingsNumeric,
    TransformerImputeMissingsString)

@pytest.mark.parametrize(
    "record, expected_valid",
    argvalues=[
        (
            pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']}),
            pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        ),
        (
            pd.DataFrame({'A': [1, 2, None], 'B': ['a', 'b', 'c']}),
            pd.DataFrame({'A': [1.0, 2.0], 'B': ['a', 'b']})
        ),
        (
            pd.DataFrame({'A': [None, None, None], 'B': ['a', 'b', 'c']}),
            pd.DataFrame({'A': [], 'B': []})
        ),
        (
            pd.DataFrame({'A': [1, 2, 3], 'B': [None, None, None]}),
            pd.DataFrame({'A': [], 'B': []})
        ),
        (
            pd.DataFrame({'A': [1, None, 3], 'B': ['a', None, 'c']}),
            pd.DataFrame({'A': [1.0, 3.0], 'B': ['a', 'c']})
        ),
        (
            pd.DataFrame({'A': [None, None, None], 'B': [None, None, None]}),
            pd.DataFrame({'A': [], 'B': []})
        ),
    ],
    ids=[
        "no missing values",
        "mixed missing values",
        "all missing values in numeric column",
        "all missing values in string column",
        "mixed missing values in both columns",
        "all missing values in both columns"
    ]
)
def test_transformer_missing(record, expected_valid):
    transformer = TransformerMissing()
    result = transformer.transform(record)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False
    )

@pytest.mark.parametrize(
    "record, threshold, expected_valid",
    [
        (
            pd.DataFrame({'A': [1, 2, None], 'B': ['a', 'b', 'c']}),
            1.0,
            pd.DataFrame({'A': [1.0, 2.0], 'B': ['a', 'b']})
        ),
        (
            pd.DataFrame({'A': [1, None, None], 'B': ['a', None, 'c']}),
            0.5,
            pd.DataFrame({'A': [1.0, None], 'B': ['a', 'c']})
        ),
        (
            pd.DataFrame({'A': [None, None], 'B': [None, None]}),
            0.0,
            pd.DataFrame({'A': [None, None], 'B': [None, None]})
        ),
        (
            pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']}),
            1.0,
            pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        ),
        (
            pd.DataFrame({'A': [1, None, 3], 'B': ['a', 'b', None], 'C': [None, 2, None], 'D': [9, None, 30]}),
            0.5,
            pd.DataFrame({'A': [1.0, None, 3.0], 'B': ['a', 'b', None], 'C': [None, 2.0, None], 'D': [9.0, None, 30.0]})
        ),
    ],
    ids=[
        "threshold_1.0_removes_any_nan",
        "threshold_0.5_keeps_partial",
        "threshold_0.0_keeps_all",
        "threshold_1.0_no_missing",
        "threshold_0.5_four_columns"
    ]
)
def test_transformer_missing_threshold(record, threshold, expected_valid):
    transformer = TransformerMissingThreshold(threshold)
    result = transformer.transform(record)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False
    )


@pytest.mark.parametrize(
    "record, expected_valid",
    [
        (
            pd.DataFrame({'A': ['  hello  ', 'world  ', '  test'], 'B': [1, 2, 3]}),
            pd.DataFrame({'A': ['hello', 'world', 'test'], 'B': [1, 2, 3]})
        ),
        (
            pd.DataFrame({'A': ['HELLO', 'World', 'TeSt'], 'B': [1, 2, 3]}),
            pd.DataFrame({'A': ['hello', 'world', 'test'], 'B': [1, 2, 3]})
        ),
        (
            pd.DataFrame({'A': ['  HELLO  ', '  World  ', 'TeSt  '], 'B': [1, 2, 3]}),
            pd.DataFrame({'A': ['hello', 'world', 'test'], 'B': [1, 2, 3]})
        ),
        (
            pd.DataFrame({'A': ['hello', 'world', 'test'], 'B': [1, 2, 3]}),
            pd.DataFrame({'A': ['hello', 'world', 'test'], 'B': [1, 2, 3]})
        ),
        (
            pd.DataFrame({'A': [1, 2, 3], 'B': [4.5, 5.5, 6.5]}),
            pd.DataFrame({'A': [1, 2, 3], 'B': [4.5, 5.5, 6.5]})
        ),
        (
            pd.DataFrame({'A': ['  TEST  ', 'Data'], 'B': ['HELLO', '  world  '], 'C': [1, 2]}),
            pd.DataFrame({'A': ['test', 'data'], 'B': ['hello', 'world'], 'C': [1, 2]})
        ),
    ],
    ids=[
        "spaces_only",
        "uppercase_only",
        "spaces_and_uppercase",
        "already_normalized",
        "numeric_only_no_change",
        "multiple_string_columns"
    ]
)
def test_transformer_normalize_strings(record, expected_valid):
    transformer = TransformerNormalizeStrings()
    result = transformer.transform(record)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False
    )


@pytest.mark.parametrize(
    "record, strategy, expected_valid",
    [
        (
            pd.DataFrame({'A': [1, 2, None, 4], 'B': ['a', 'b', 'c', 'd']}),
            'mean',
            pd.DataFrame({'A': [1.0, 2.0, 2.333333, 4.0], 'B': ['a', 'b', 'c', 'd']})
        ),
        (
            pd.DataFrame({'A': [1, 2, None, 4, 10], 'B': ['a', 'b', 'c', 'd', 'e']}),
            'median',
            pd.DataFrame({'A': [1.0, 2.0, 3.0, 4.0, 10.0], 'B': ['a', 'b', 'c', 'd', 'e']})
        ),
        (
            pd.DataFrame({'A': [1, 2, 2, None, 4], 'B': ['a', 'b', 'c', 'd', 'e']}),
            'mode',
            pd.DataFrame({'A': [1.0, 2.0, 2.0, 2.0, 4.0], 'B': ['a', 'b', 'c', 'd', 'e']})
        ),
        (
            pd.DataFrame({'A': [1, None, 3], 'B': [10, 20, None], 'C': ['x', 'y', 'z']}),
            'mean',
            pd.DataFrame({'A': [1.0, 2.0, 3.0], 'B': [10.0, 20.0, 15.0], 'C': ['x', 'y', 'z']})
        ),
        (
            pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']}),
            'mean',
            pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        ),
    ],
    ids=[
        "mean_strategy",
        "median_strategy",
        "mode_strategy",
        "mean_multiple_numeric_columns",
        "no_missing_values"
    ]
)
def test_transformer_impute_missings_numeric(record, strategy, expected_valid):
    transformer = TransformerImputeMissingsNumeric(strategy)
    result = transformer.transform(record)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False,
        atol=1e-5
    )


def test_transformer_impute_missings_numeric_invalid_strategy():
    transformer = TransformerImputeMissingsNumeric('invalid')
    data = pd.DataFrame({'A': [1, 2, None]})
    with pytest.raises(ValueError, match="Unknown imputation strategy: invalid"):
        transformer.transform(data)


@pytest.mark.parametrize(
    "record, strategy, default_value, expected_valid",
    [
        (
            pd.DataFrame({'A': ['a', 'b', 'a', None, 'a'], 'B': [1, 2, 3, 4, 5]}),
            'mode',
            None,
            pd.DataFrame({'A': ['a', 'b', 'a', 'a', 'a'], 'B': [1, 2, 3, 4, 5]})
        ),
        (
            pd.DataFrame({'A': ['a', 'b', None, 'd'], 'B': [1, 2, 3, 4]}),
            'default',
            'unknown',
            pd.DataFrame({'A': ['a', 'b', 'unknown', 'd'], 'B': [1, 2, 3, 4]})
        ),
        (
            pd.DataFrame({'A': ['x', 'x', None], 'B': ['y', None, 'y'], 'C': [1, 2, 3]}),
            'mode',
            None,
            pd.DataFrame({'A': ['x', 'x', 'x'], 'B': ['y', 'y', 'y'], 'C': [1, 2, 3]})
        ),
        (
            pd.DataFrame({'A': ['a', None, 'c'], 'B': [1, 2, 3]}),
            'default',
            0,
            pd.DataFrame({'A': ['a', 0, 'c'], 'B': [1, 2, 3]})
        ),
    ],
    ids=[
        "mode_strategy",
        "default_strategy_string",
        "mode_multiple_columns",
        "default_strategy_numeric_value"
    ]
)
def test_transformer_impute_missings_string(record, strategy, default_value, expected_valid):
    transformer = TransformerImputeMissingsString(strategy, default_value)
    result = transformer.transform(record)
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False
    )


def test_transformer_impute_missings_string_invalid_strategy():
    transformer = TransformerImputeMissingsString('invalid')
    data = pd.DataFrame({'A': ['a', 'b', None]})
    with pytest.raises(ValueError, match="Unknown imputation strategy: invalid"):
        transformer.transform(data)
