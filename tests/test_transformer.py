import pytest
import pandas as pd
import logging

from unidad1_project import (TransformerMissing,
    TransformerMissingThreshold,
    TransformerNormalizeStrings,
    TransformerImputeMissingsNumeric,
    TransformerImputeMissingsString,
    TransformerFilterRows,
    TransformerSelectColumns,
    TransformerGroupByAggregate
    )

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

    with pytest.raises(ValueError, match="Strategy must be one of 'mean', 'median', or 'mode'."):
        TransformerImputeMissingsNumeric('invalid')

    # Verify error log during initialization
    assert "TransformerImputeMissingsNumeric: Invalid strategy 'invalid'" in caplog.text


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

    with pytest.raises(ValueError, match="Strategy must be one of 'mode' or 'default'."):
        TransformerImputeMissingsString('invalid')

    # Verify error log during initialization
    assert "TransformerImputeMissingsString: Invalid strategy 'invalid'" in caplog.text


def test_transformer_impute_missings_string_default_none_warning(caplog):
    """Test warning when using default strategy with None value."""
    caplog.set_level(logging.WARNING)

    TransformerImputeMissingsString('default', None)

    # Verify warning was logged
    assert "TransformerImputeMissingsString: Using 'default' strategy with default_value=None" in caplog.text
    assert "may not produce expected results" in caplog.text

@pytest.mark.parametrize(
    "data, column, operator, value, expected_rows",
    [
        (
            pd.DataFrame({'age': [25, 30, 35, 40], 'name': ['A', 'B', 'C', 'D']}),
            'age',
            '>',
            30,
            2  # 35, 40
        ),
        (
            pd.DataFrame({'age': [25, 30, 35, 40], 'name': ['A', 'B', 'C', 'D']}),
            'age',
            '>=',
            30,
            3  # 30, 35, 40
        ),
        (
            pd.DataFrame({'age': [25, 30, 35, 40], 'name': ['A', 'B', 'C', 'D']}),
            'age',
            '<',
            35,
            2  # 25, 30
        ),
        (
            pd.DataFrame({'age': [25, 30, 35, 40], 'name': ['A', 'B', 'C', 'D']}),
            'age',
            '<=',
            35,
            3  # 25, 30, 35
        ),
        (
            pd.DataFrame({'age': [25, 30, 35, 40], 'name': ['A', 'B', 'C', 'D']}),
            'age',
            '==',
            30,
            1  # 30
        ),
        (
            pd.DataFrame({'age': [25, 30, 35, 40], 'name': ['A', 'B', 'C', 'D']}),
            'age',
            '!=',
            30,
            3  # 25, 35, 40
        ),
        (
            pd.DataFrame({'city': ['NY', 'LA', 'SF', 'NY'], 'pop': [100, 200, 150, 110]}),
            'city',
            'in',
            ['NY', 'SF'],
            3  # NY, SF, NY
        ),
        (
            pd.DataFrame({'city': ['NY', 'LA', 'SF', 'NY'], 'pop': [100, 200, 150, 110]}),
            'city',
            'not_in',
            ['LA'],
            3  # NY, SF, NY
        ),
        (
            pd.DataFrame({'text': ['hello world', 'test', 'hello test', 'other']}),
            'text',
            'contains',
            'hello',
            2  # 'hello world', 'hello test'
        ),
    ],
    ids=[
        "greater_than",
        "greater_equal",
        "less_than",
        "less_equal",
        "equals",
        "not_equals",
        "in_list",
        "not_in_list",
        "contains_string"
    ]
)
def test_transformer_filter_rows(caplog, data, column, operator, value, expected_rows):
    """Test TransformerFilterRows with various operators."""
    caplog.set_level(logging.DEBUG)

    transformer = TransformerFilterRows(column=column, operator=operator, value=value)
    result = transformer.transform(data)

    # Verify correct number of rows
    assert len(result) == expected_rows

    # Verify logs
    assert "TransformerFilterRows: Processing DataFrame with shape" in caplog.text
    assert f"TransformerFilterRows: Filtering column '{column}'" in caplog.text
    assert f"with operator '{operator}'" in caplog.text
    assert "TransformerFilterRows: Kept" in caplog.text
    assert "rows" in caplog.text


def test_transformer_filter_rows_custom_condition(caplog):
    """Test TransformerFilterRows with custom condition."""
    caplog.set_level(logging.INFO)

    data = pd.DataFrame({'age': [25, 30, 35, 40], 'salary': [50000, 60000, 70000, 80000]})

    # Custom condition: age > 28 AND salary > 65000
    def condition(df):
        return (df['age'] > 28) & (df['salary'] > 65000)

    transformer = TransformerFilterRows(condition=condition)
    result = transformer.transform(data)

    # Should keep rows with age 35 and 40
    assert len(result) == 2
    assert list(result['age']) == [35, 40]

    # Verify logs
    assert "TransformerFilterRows: Applying custom condition" in caplog.text


def test_transformer_filter_rows_no_matches(caplog):
    """Test TransformerFilterRows when no rows match."""
    caplog.set_level(logging.INFO)

    data = pd.DataFrame({'age': [25, 30, 35], 'name': ['A', 'B', 'C']})
    transformer = TransformerFilterRows(column='age', operator='>', value=100)

    result = transformer.transform(data)

    # Should return empty DataFrame
    assert len(result) == 0
    assert list(result.columns) == list(data.columns)

    # Verify logs show 100% removal
    assert "removed 3 rows (100.00%)" in caplog.text


def test_transformer_filter_rows_all_match(caplog):
    """Test TransformerFilterRows when all rows match."""
    caplog.set_level(logging.INFO)

    data = pd.DataFrame({'age': [25, 30, 35], 'name': ['A', 'B', 'C']})
    transformer = TransformerFilterRows(column='age', operator='>', value=20)

    result = transformer.transform(data)

    # Should keep all rows
    assert len(result) == len(data)

    # Verify logs show 0% removal
    assert "removed 0 rows (0.00%)" in caplog.text


def test_transformer_filter_rows_invalid_column(caplog):
    """Test TransformerFilterRows with non-existent column."""
    caplog.set_level(logging.ERROR)

    data = pd.DataFrame({'age': [25, 30, 35]})
    transformer = TransformerFilterRows(column='invalid_col', operator='>', value=30)

    with pytest.raises(ValueError, match="Column 'invalid_col' not found"):
        transformer.transform(data)

    # Verify error log
    assert "TransformerFilterRows: Column 'invalid_col' not found" in caplog.text


def test_transformer_filter_rows_no_operator_or_condition(caplog):
    """Test TransformerFilterRows initialization without operator or condition."""
    caplog.set_level(logging.ERROR)

    with pytest.raises(ValueError, match="Must provide either operator or condition"):
        TransformerFilterRows(column='age')

    # Verify error log
    assert "TransformerFilterRows: Must provide either operator or condition" in caplog.text


def test_transformer_filter_rows_invalid_operator(caplog):
    """Test TransformerFilterRows with invalid operator."""
    caplog.set_level(logging.ERROR)

    with pytest.raises(ValueError, match="Invalid operator"):
        TransformerFilterRows(column='age', operator='invalid', value=30)

    # Verify error log
    assert "TransformerFilterRows: Invalid operator 'invalid'" in caplog.text


def test_transformer_filter_rows_operator_without_column(caplog):
    """Test TransformerFilterRows with operator but no column."""
    caplog.set_level(logging.ERROR)

    with pytest.raises(ValueError, match="Column must be specified when using operator"):
        TransformerFilterRows(operator='>', value=30)

    # Verify error log
    assert "TransformerFilterRows: Column must be specified when using operator" in caplog.text

@pytest.mark.parametrize(
    "data, columns, expected_columns",
    [
        (
            pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]}),
            ['A', 'C'],
            ['A', 'C']
        ),
        (
            pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]}),
            ['B'],
            ['B']
        ),
        (
            pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]}),
            ['C', 'A', 'B'],
            ['C', 'A', 'B']  # Order matters
        ),
    ],
    ids=[
        "select_multiple",
        "select_single",
        "reorder_columns"
    ]
)
def test_transformer_select_columns(caplog, data, columns, expected_columns):
    """Test TransformerSelectColumns with column selection."""
    caplog.set_level(logging.DEBUG)

    transformer = TransformerSelectColumns(columns=columns)
    result = transformer.transform(data)

    # Verify correct columns
    assert list(result.columns) == expected_columns
    assert len(result) == len(data)

    # Verify logs
    assert "TransformerSelectColumns: Processing DataFrame with shape" in caplog.text
    assert f"TransformerSelectColumns: Selecting {len(columns)} columns" in caplog.text
    assert "TransformerSelectColumns: Output has" in caplog.text


@pytest.mark.parametrize(
    "data, drop, expected_columns",
    [
        (
            pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]}),
            ['B'],
            ['A', 'C']
        ),
        (
            pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]}),
            ['A', 'C'],
            ['B']
        ),
        (
            pd.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6], 'D': [7, 8]}),
            ['B', 'D'],
            ['A', 'C']
        ),
    ],
    ids=[
        "drop_single",
        "drop_multiple",
        "drop_alternating"
    ]
)
def test_transformer_select_columns_drop(caplog, data, drop, expected_columns):
    """Test TransformerSelectColumns with column dropping."""
    caplog.set_level(logging.DEBUG)

    transformer = TransformerSelectColumns(drop=drop)
    result = transformer.transform(data)

    # Verify correct columns remain
    assert list(result.columns) == expected_columns
    assert len(result) == len(data)

    # Verify logs
    assert f"TransformerSelectColumns: Dropping {len(drop)} columns" in caplog.text


def test_transformer_select_columns_invalid_column(caplog):
    """Test TransformerSelectColumns with non-existent column."""
    caplog.set_level(logging.ERROR)

    data = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    transformer = TransformerSelectColumns(columns=['A', 'Z'])

    with pytest.raises(ValueError, match="Columns not found in DataFrame"):
        transformer.transform(data)

    # Verify error log
    assert "TransformerSelectColumns: Columns not found: {'Z'}" in caplog.text


def test_transformer_select_columns_drop_nonexistent(caplog):
    """Test TransformerSelectColumns dropping non-existent columns."""
    caplog.set_level(logging.WARNING)

    data = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    transformer = TransformerSelectColumns(drop=['B', 'Z'])

    result = transformer.transform(data)

    # Should drop B and ignore Z
    assert list(result.columns) == ['A']

    # Verify warning log
    assert "TransformerSelectColumns: Columns to drop not found: {'Z'}" in caplog.text


def test_transformer_select_columns_both_params_error(caplog):
    """Test TransformerSelectColumns with both columns and drop specified."""
    caplog.set_level(logging.ERROR)

    with pytest.raises(ValueError, match="Cannot specify both 'columns' and 'drop'"):
        TransformerSelectColumns(columns=['A'], drop=['B'])

    # Verify error log
    assert "TransformerSelectColumns: Cannot specify both columns and drop" in caplog.text


def test_transformer_select_columns_no_params_error(caplog):
    """Test TransformerSelectColumns without any parameters."""
    caplog.set_level(logging.ERROR)

    with pytest.raises(ValueError, match="Must specify either 'columns' or 'drop'"):
        TransformerSelectColumns()

    # Verify error log
    assert "TransformerSelectColumns: Must specify either columns or drop" in caplog.text


def test_transformer_groupby_aggregate_single_column(caplog):
    """Test TransformerGroupByAggregate with single group column."""
    caplog.set_level(logging.DEBUG)

    data = pd.DataFrame({
        'category': ['A', 'A', 'B', 'B', 'B'],
        'sales': [100, 150, 200, 250, 300],
        'quantity': [10, 15, 20, 25, 30]
    })

    transformer = TransformerGroupByAggregate(
        group_by='category',
        aggregations={'sales': 'sum', 'quantity': 'mean'}
    )
    result = transformer.transform(data)

    # Verify aggregation
    assert len(result) == 2  # Two categories
    assert set(result['category']) == {'A', 'B'}

    # Verify values - Separar en pasos para mejor inferencia de tipos
    a_rows = result[result['category'] == 'A']
    b_rows = result[result['category'] == 'B']
    a_sales = int(a_rows['sales'].tolist()[0]) if len(a_rows) > 0 else 0
    b_sales = int(b_rows['sales'].tolist()[0]) if len(b_rows) > 0 else 0
    assert a_sales == 250  # 100 + 150
    assert b_sales == 750  # 200 + 250 + 300

    # Verify logs
    assert "TransformerGroupByAggregate: Processing DataFrame with shape" in caplog.text
    assert "TransformerGroupByAggregate: Grouping by ['category']" in caplog.text
    assert "TransformerGroupByAggregate: Reduced from 5 rows to 2 groups" in caplog.text


def test_transformer_groupby_aggregate_multiple_columns(caplog):
    """Test TransformerGroupByAggregate with multiple group columns."""
    caplog.set_level(logging.DEBUG)

    data = pd.DataFrame({
        'category': ['A', 'A', 'A', 'B', 'B'],
        'region': ['East', 'East', 'West', 'East', 'West'],
        'sales': [100, 150, 200, 250, 300]
    })

    transformer = TransformerGroupByAggregate(
        group_by=['category', 'region'],
        aggregations={'sales': 'sum'}
    )
    result = transformer.transform(data)

    # Should have 3 groups: A-East, A-West, B-East, B-West
    assert len(result) == 4

    # Verify logs
    assert "TransformerGroupByAggregate: Grouping by ['category', 'region']" in caplog.text


def test_transformer_groupby_aggregate_multiple_functions(caplog):
    """Test TransformerGroupByAggregate with multiple aggregation functions."""
    caplog.set_level(logging.DEBUG)

    data = pd.DataFrame({
        'category': ['A', 'A', 'B', 'B'],
        'sales': [100, 200, 150, 250]
    })

    transformer = TransformerGroupByAggregate(
        group_by='category',
        aggregations={'sales': ['sum', 'mean', 'max', 'min']}
    )
    result = transformer.transform(data)

    # Verify multi-level aggregation
    assert len(result) == 2

    # Column names should be flattened
    assert any('sum' in str(col) for col in result.columns)
    assert any('mean' in str(col) for col in result.columns)

    # Verify logs
    assert "TransformerGroupByAggregate: Flattening multi-level columns" in caplog.text


def test_transformer_groupby_aggregate_various_functions(caplog):
    """Test TransformerGroupByAggregate with various aggregation functions."""
    caplog.set_level(logging.DEBUG)

    data = pd.DataFrame({
        'group': ['X', 'X', 'X', 'Y', 'Y'],
        'value': [10, 20, 30, 40, 50]
    })

    transformer = TransformerGroupByAggregate(
        group_by='group',
        aggregations={'value': ['sum', 'mean', 'median', 'min', 'max', 'count', 'std']}
    )
    result = transformer.transform(data)

    # Verify result
    assert len(result) == 2

    # Verify logs
    assert "TransformerGroupByAggregate: Grouping by ['group']" in caplog.text


def test_transformer_groupby_aggregate_no_reset_index(caplog):
    """Test TransformerGroupByAggregate without resetting index."""
    caplog.set_level(logging.DEBUG)

    data = pd.DataFrame({
        'category': ['A', 'A', 'B', 'B'],
        'sales': [100, 200, 150, 250]
    })

    transformer = TransformerGroupByAggregate(
        group_by='category',
        aggregations={'sales': 'sum'},
        reset_index=False
    )
    result = transformer.transform(data)

    # Index should be 'category'
    assert result.index.name == 'category'

    # Should not see reset index log
    assert "TransformerGroupByAggregate: Resetting index" not in caplog.text


def test_transformer_groupby_aggregate_invalid_group_column(caplog):
    """Test TransformerGroupByAggregate with non-existent group column."""
    caplog.set_level(logging.ERROR)

    data = pd.DataFrame({'category': ['A', 'B'], 'sales': [100, 200]})
    transformer = TransformerGroupByAggregate(
        group_by='invalid_column',
        aggregations={'sales': 'sum'}
    )

    with pytest.raises(ValueError, match="Group-by columns not found"):
        transformer.transform(data)

    # Verify error log
    assert "TransformerGroupByAggregate: Group-by columns not found: {'invalid_column'}" in caplog.text


def test_transformer_groupby_aggregate_invalid_agg_column(caplog):
    """Test TransformerGroupByAggregate with non-existent aggregation column."""
    caplog.set_level(logging.ERROR)

    data = pd.DataFrame({'category': ['A', 'B'], 'sales': [100, 200]})
    transformer = TransformerGroupByAggregate(
        group_by='category',
        aggregations={'invalid_col': 'sum'}
    )

    with pytest.raises(ValueError, match="Aggregation columns not found"):
        transformer.transform(data)

    # Verify error log
    assert "TransformerGroupByAggregate: Aggregation columns not found: {'invalid_col'}" in caplog.text


def test_transformer_groupby_aggregate_empty_groups(caplog):
    """Test TransformerGroupByAggregate that results in empty groups."""
    caplog.set_level(logging.INFO)

    data = pd.DataFrame({
        'category': ['A'],
        'sales': [100]
    })

    transformer = TransformerGroupByAggregate(
        group_by='category',
        aggregations={'sales': 'sum'}
    )
    result = transformer.transform(data)

    # Should have 1 group
    assert len(result) == 1
    assert result['sales'].iloc[0] == 100


def test_transformer_groupby_aggregate_complex_scenario(caplog):
    """Test TransformerGroupByAggregate with complex real-world scenario."""
    caplog.set_level(logging.INFO)

    data = pd.DataFrame({
        'store': ['Store1', 'Store1', 'Store2', 'Store2', 'Store1'],
        'product': ['A', 'B', 'A', 'B', 'A'],
        'sales': [100, 200, 150, 250, 120],
        'quantity': [10, 20, 15, 25, 12],
        'price': [10.0, 10.0, 10.0, 10.0, 10.0]
    })

    transformer = TransformerGroupByAggregate(
        group_by=['store', 'product'],
        aggregations={
            'sales': ['sum', 'mean'],
            'quantity': 'sum',
            'price': 'first'
        }
    )
    result = transformer.transform(data)

    # Should have 4 unique store-product combinations
    assert len(result) == 4

    # Verify logs
    assert "TransformerGroupByAggregate: Reduced from 5 rows to 4 groups" in caplog.text
