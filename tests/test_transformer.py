import pytest
import pandas as pd
import logging

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
def test_transformer_missing(caplog, record, expected_valid):
    """Test TransformerMissing with log verification."""
    caplog.set_level(logging.DEBUG)

    transformer = TransformerMissing()
    result = transformer.transform(record)

    # Verify DataFrame transformation
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False
    )

    # Verify logs
    assert "TransformerMissing: Processing DataFrame with shape" in caplog.text
    assert "TransformerMissing: Total missing values:" in caplog.text

    if len(result) < len(record):
        assert "TransformerMissing: Removed" in caplog.text
        assert "rows" in caplog.text
    else:
        assert "TransformerMissing: No rows removed (no missing values found)" in caplog.text

    assert "TransformerMissing: Output shape" in caplog.text


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
def test_transformer_missing_threshold(caplog, record, threshold, expected_valid):
    """Test TransformerMissingThreshold with log verification."""
    caplog.set_level(logging.DEBUG)

    transformer = TransformerMissingThreshold(threshold)

    # Verify initialization log
    assert f"TransformerMissingThreshold initialized with threshold={threshold}" in caplog.text

    caplog.clear()
    result = transformer.transform(record)

    # Verify DataFrame transformation
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False
    )

    # Verify transform logs
    assert "TransformerMissingThreshold: Processing DataFrame with shape" in caplog.text
    assert f"TransformerMissingThreshold: Threshold={threshold}" in caplog.text
    assert "columns=" in caplog.text
    assert "min_non_null=" in caplog.text

    if len(result) < len(record):
        assert "TransformerMissingThreshold: Removed" in caplog.text
    else:
        assert "TransformerMissingThreshold: No rows removed" in caplog.text

    assert "TransformerMissingThreshold: Output shape" in caplog.text


def test_transformer_missing_threshold_invalid_threshold(caplog):
    """Test TransformerMissingThreshold with invalid threshold."""
    caplog.set_level(logging.WARNING)

    with pytest.raises(ValueError, match="Threshold must be between 0.0 and 1.0"):
        TransformerMissingThreshold(1.5)

    # Verify warning log was generated
    assert "TransformerMissingThreshold: Threshold 1.5 is outside recommended range" in caplog.text


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
def test_transformer_normalize_strings(caplog, record, expected_valid):
    """Test TransformerNormalizeStrings with log verification."""
    caplog.set_level(logging.DEBUG)

    transformer = TransformerNormalizeStrings()
    result = transformer.transform(record)

    # Verify DataFrame transformation
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False
    )

    # Verify logs
    assert "TransformerNormalizeStrings: Processing DataFrame with shape" in caplog.text

    string_columns = record.select_dtypes(include='str').columns.tolist()

    if not string_columns:
        assert "TransformerNormalizeStrings: No string columns found to normalize" in caplog.text
    else:
        assert f"TransformerNormalizeStrings: Normalizing {len(string_columns)}" in caplog.text
        assert "string columns:" in caplog.text
        assert "TransformerNormalizeStrings: Successfully normalized all string columns" in caplog.text

        # Verify each column was logged
        for col in string_columns:
            assert f"TransformerNormalizeStrings: Processing column '{col}'" in caplog.text


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
def test_transformer_impute_missings_numeric(caplog, record, strategy, expected_valid):
    """Test TransformerImputeMissingsNumeric with log verification."""
    caplog.set_level(logging.DEBUG)

    transformer = TransformerImputeMissingsNumeric(strategy)

    # Verify initialization log
    assert f"TransformerImputeMissingsNumeric initialized with strategy='{strategy}'" in caplog.text

    caplog.clear()
    result = transformer.transform(record)

    # Verify DataFrame transformation
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False,
        atol=1e-5
    )

    # Verify transform logs
    assert "TransformerImputeMissingsNumeric: Processing DataFrame with shape" in caplog.text
    assert "TransformerImputeMissingsNumeric: Found" in caplog.text
    assert "numeric columns" in caplog.text

    numeric_cols = record.select_dtypes(include='number').columns.tolist()
    missing_before = record[numeric_cols].isnull().sum().sum() if numeric_cols else 0

    if missing_before == 0:
        assert "TransformerImputeMissingsNumeric: No missing values in numeric columns" in caplog.text
    else:
        assert f"TransformerImputeMissingsNumeric: Imputing {missing_before}" in caplog.text
        assert f"using {strategy} strategy" in caplog.text
        assert "TransformerImputeMissingsNumeric: Successfully imputed" in caplog.text


def test_transformer_impute_missings_numeric_invalid_strategy(caplog):
    """Test TransformerImputeMissingsNumeric with invalid strategy."""
    caplog.set_level(logging.ERROR)

    transformer = TransformerImputeMissingsNumeric('invalid')

    # Verify error log during initialization
    assert "TransformerImputeMissingsNumeric: Invalid strategy 'invalid'" in caplog.text

    caplog.clear()
    data = pd.DataFrame({'A': [1, 2, None]})

    with pytest.raises(ValueError, match="Unknown imputation strategy: invalid"):
        transformer.transform(data)

    # Verify error log during transform
    assert "TransformerImputeMissingsNumeric: Unknown strategy 'invalid'" in caplog.text


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
def test_transformer_impute_missings_string(caplog, record, strategy, default_value, expected_valid):
    """Test TransformerImputeMissingsString with log verification."""
    caplog.set_level(logging.DEBUG)

    transformer = TransformerImputeMissingsString(strategy, default_value)

    # Verify initialization log
    assert f"TransformerImputeMissingsString initialized with strategy='{strategy}'" in caplog.text
    assert f"default_value={default_value}" in caplog.text

    # Check for warning if default strategy with None value
    if strategy == 'default' and default_value is None:
        assert "TransformerImputeMissingsString: Using 'default' strategy with default_value=None" in caplog.text

    caplog.clear()
    result = transformer.transform(record)

    # Verify DataFrame transformation
    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected_valid.reset_index(drop=True),
        check_dtype=False
    )

    # Verify transform logs
    assert "TransformerImputeMissingsString: Processing DataFrame with shape" in caplog.text
    assert "TransformerImputeMissingsString: Found" in caplog.text
    assert "object/string columns" in caplog.text

    string_cols = record.select_dtypes(include=['object', 'string']).columns.tolist()
    missing_before = record[string_cols].isnull().sum().sum() if string_cols else 0

    if missing_before == 0:
        assert "TransformerImputeMissingsString: No missing values in string columns" in caplog.text
    else:
        assert f"TransformerImputeMissingsString: Imputing {missing_before}" in caplog.text

        if strategy == 'mode':
            assert "using mode strategy" in caplog.text
        elif strategy == 'default':
            assert f"with default value: '{default_value}'" in caplog.text

        assert "TransformerImputeMissingsString: Successfully imputed" in caplog.text


def test_transformer_impute_missings_string_invalid_strategy(caplog):
    """Test TransformerImputeMissingsString with invalid strategy."""
    caplog.set_level(logging.ERROR)

    transformer = TransformerImputeMissingsString('invalid')

    # Verify error log during initialization
    assert "TransformerImputeMissingsString: Invalid strategy 'invalid'" in caplog.text

    caplog.clear()
    data = pd.DataFrame({'A': ['a', 'b', None]})

    with pytest.raises(ValueError, match="Unknown imputation strategy: invalid"):
        transformer.transform(data)

    # Verify error log during transform
    assert "TransformerImputeMissingsString: Unknown strategy 'invalid'" in caplog.text


def test_transformer_impute_missings_string_default_none_warning(caplog):
    """Test warning when using default strategy with None value."""
    caplog.set_level(logging.WARNING)

    TransformerImputeMissingsString('default', None)

    # Verify warning was logged
    assert "TransformerImputeMissingsString: Using 'default' strategy with default_value=None" in caplog.text
    assert "may not produce expected results" in caplog.text
