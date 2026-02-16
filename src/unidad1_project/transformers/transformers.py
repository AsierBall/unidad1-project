from typing import Dict, Protocol, Any, Callable, List, Union
import pandas as pd
from ..logging import get_logger

logger = get_logger(__name__)

class Transformer(Protocol):
    """
    Protocol defining the interface for data transformers.

    Classes implementing this protocol must provide a transform method
    that processes a pandas DataFrame.
    """

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the DataFrame data.

        :param data: DataFrame to transform
        :type data: pd.DataFrame
        :return: Transformed DataFrame
        :rtype: pd.DataFrame
        """
        ...

class TransformerMissing(Transformer):
    """
    Transformer that removes all rows with missing values.

    This transformer applies dropna() without a threshold, removing any
    row that contains at least one NaN value.
    """

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Remove all rows containing missing values.

        :param data: DataFrame to process
        :type data: pd.DataFrame
        :return: DataFrame without rows containing missing values
        :rtype: pd.DataFrame
        """
        initial_rows = len(data)
        initial_missing = data.isnull().sum().sum()

        logger.debug(f"TransformerMissing: Processing DataFrame with shape {data.shape}")
        logger.debug(f"TransformerMissing: Total missing values: {initial_missing}")

        result = data.dropna()
        removed_rows = initial_rows - len(result)

        if removed_rows > 0:
            logger.info(
                f"TransformerMissing: Removed {removed_rows} rows "
                f"({removed_rows/initial_rows*100:.2f}%) with missing values"
            )
        else:
            logger.info("TransformerMissing: No rows removed (no missing values found)")

        logger.debug(f"TransformerMissing: Output shape {result.shape}")
        return result

class TransformerMissingThreshold(Transformer):
    """
    Transformer that removes rows based on a missing values threshold.

    Rows are kept only if they have at least a minimum number of non-NaN
    values, determined by the threshold multiplied by the number of columns.
    """

    def __init__(self, threshold: float):
        """
        Initialize the transformer with a threshold.

        :param threshold: Proportion of non-NaN values required (0.0 to 1.0)
        :type threshold: float
        """
        self._threshold = threshold
        logger.debug(
            f"TransformerMissingThreshold initialized with threshold={threshold}"
        )

        if not 0.0 <= threshold <= 1.0:
            logger.error(
                f"TransformerMissingThreshold: Threshold {threshold} is outside "
                f"recommended range [0.0, 1.0]"
            )
            raise ValueError("Threshold must be between 0.0 and 1.0.")

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Remove rows that don't meet the non-NaN values threshold.

        :param data: DataFrame to process
        :type data: pd.DataFrame
        :return: DataFrame filtered by threshold
        :rtype: pd.DataFrame
        """
        initial_rows = len(data)
        n_cols = len(data.columns)
        min_non_null = self._threshold * n_cols

        logger.debug(
            f"TransformerMissingThreshold: Processing DataFrame with shape {data.shape}"
        )
        logger.debug(
            f"TransformerMissingThreshold: Threshold={self._threshold}, "
            f"columns={n_cols}, min_non_null={min_non_null:.2f}"
        )

        result = data.dropna(thresh=min_non_null)
        removed_rows = initial_rows - len(result)

        if removed_rows > 0:
            logger.info(
                f"TransformerMissingThreshold: Removed {removed_rows} rows "
                f"({removed_rows/initial_rows*100:.2f}%) not meeting threshold"
            )
        else:
            logger.info(
                "TransformerMissingThreshold: No rows removed "
                "(all rows meet threshold)"
            )

        logger.debug(f"TransformerMissingThreshold: Output shape {result.shape}")
        return result

class TransformerNormalizeStrings(Transformer):
    """
    Transformer that normalizes string-type columns.

    Applies strip() to remove whitespace and lower() to convert to lowercase
    on all string-type columns in the DataFrame.
    """

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize all string-type columns by removing whitespace and
        converting to lowercase.

        :param data: DataFrame to normalize
        :type data: pd.DataFrame
        :return: DataFrame with normalized strings
        :rtype: pd.DataFrame
        """
        logger.debug(
            f"TransformerNormalizeStrings: Processing DataFrame with shape {data.shape}"
        )

        string_columns = data.select_dtypes(include='str').columns.tolist()

        if not string_columns:
            logger.info("TransformerNormalizeStrings: No string columns found to normalize")
            return data

        logger.info(
            f"TransformerNormalizeStrings: Normalizing {len(string_columns)} "
            f"string columns: {string_columns}"
        )

        for column in string_columns:
            logger.debug(f"TransformerNormalizeStrings: Processing column '{column}'")
            data[column] = data[column].str.strip().str.lower()

        logger.info("TransformerNormalizeStrings: Successfully normalized all string columns")
        return data


class TransformerImputeMissingsNumeric(Transformer):
    """
    Transformer that imputes missing values in numeric columns.

    Supports different imputation strategies: mean, median, or mode.
    """

    def __init__(self, strategy: str = 'mean'):
        """
        Initialize the transformer with an imputation strategy.

        :param strategy: Imputation strategy ('mean', 'median', 'mode')
        :type strategy: str
        """
        self._strategy = strategy
        logger.debug(
            f"TransformerImputeMissingsNumeric initialized with strategy='{strategy}'"
        )

        valid_strategies = ['mean', 'median', 'mode']
        if strategy not in valid_strategies:
            logger.error(
                f"TransformerImputeMissingsNumeric: Invalid strategy '{strategy}'. "
                f"Valid options: {valid_strategies}"
            )

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Impute missing values in numeric columns according to the strategy.

        :param data: DataFrame to process
        :type data: pd.DataFrame
        :return: DataFrame with imputed numeric values
        :rtype: pd.DataFrame
        :raises ValueError: If the strategy is not valid
        """
        logger.debug(
            f"TransformerImputeMissingsNumeric: Processing DataFrame with shape {data.shape}"
        )

        numeric_cols = data.select_dtypes(include='number').columns.tolist()
        missing_before = data[numeric_cols].isnull().sum().sum() if numeric_cols else 0

        logger.debug(
            f"TransformerImputeMissingsNumeric: Found {len(numeric_cols)} numeric columns "
            f"with {missing_before} missing values"
        )

        if missing_before == 0:
            logger.info(
                "TransformerImputeMissingsNumeric: No missing values in numeric columns"
            )
            return data

        try:
            if self._strategy == 'mean':
                logger.info(
                    f"TransformerImputeMissingsNumeric: Imputing {missing_before} "
                    f"missing values using mean strategy"
                )
                result = data.fillna(data.mean(numeric_only=True))
            elif self._strategy == 'median':
                logger.info(
                    f"TransformerImputeMissingsNumeric: Imputing {missing_before} "
                    f"missing values using median strategy"
                )
                result = data.fillna(data.median(numeric_only=True))
            elif self._strategy == 'mode':
                logger.info(
                    f"TransformerImputeMissingsNumeric: Imputing {missing_before} "
                    f"missing values using mode strategy"
                )
                result = data.fillna(data.mode().iloc[0])
            else:
                logger.error(
                    f"TransformerImputeMissingsNumeric: Unknown strategy '{self._strategy}'"
                )
                raise ValueError(f"Unknown imputation strategy: {self._strategy}")

            missing_after = result[numeric_cols].isnull().sum().sum()
            imputed_count = missing_before - missing_after

            logger.info(
                f"TransformerImputeMissingsNumeric: Successfully imputed {imputed_count} "
                f"values ({missing_after} missing values remaining)"
            )

            return result

        except Exception:
            logger.exception(
                f"TransformerImputeMissingsNumeric: Error during imputation with "
                f"strategy '{self._strategy}'"
            )
            raise

class TransformerImputeMissingsString(Transformer):
    """
    Transformer that imputes missing values in string-type columns.

    Supports imputation by mode or by a specified default value.
    """

    def __init__(self, strategy: str = 'mode', default_value: Any = None):
        """
        Initialize the transformer with a strategy and default value.

        :param strategy: Imputation strategy ('mode', 'default')
        :type strategy: str
        :param default_value: Default value for 'default' strategy
        :type default_value: Any
        """
        self._strategy = strategy
        self._default_value = default_value

        logger.debug(
            f"TransformerImputeMissingsString initialized with strategy='{strategy}', "
            f"default_value={default_value}"
        )

        valid_strategies = ['mode', 'default']
        if strategy not in valid_strategies:
            logger.error(
                f"TransformerImputeMissingsString: Invalid strategy '{strategy}'. "
                f"Valid options: {valid_strategies}"
            )

        if strategy == 'default' and default_value is None:
            logger.warning(
                "TransformerImputeMissingsString: Using 'default' strategy with "
                "default_value=None may not produce expected results"
            )

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Impute missing values in string columns according to the strategy.

        :param data: DataFrame to process
        :type data: pd.DataFrame
        :return: DataFrame with imputed string values
        :rtype: pd.DataFrame
        :raises ValueError: If the strategy is not valid
        """
        logger.debug(
            f"TransformerImputeMissingsString: Processing DataFrame with shape {data.shape}"
        )

        # Count missing values in object/string columns
        string_cols = data.select_dtypes(include=['object', 'string']).columns.tolist()
        missing_before = data[string_cols].isnull().sum().sum() if string_cols else 0

        logger.debug(
            f"TransformerImputeMissingsString: Found {len(string_cols)} "
            f"object/string columns with {missing_before} missing values"
        )

        if missing_before == 0:
            logger.info(
                "TransformerImputeMissingsString: No missing values in string columns"
            )
            return data

        try:
            if self._strategy == 'mode':
                logger.info(
                    f"TransformerImputeMissingsString: Imputing {missing_before} "
                    f"missing values using mode strategy"
                )
                result = data.fillna(data.mode().iloc[0])
            elif self._strategy == 'default':
                logger.info(
                    f"TransformerImputeMissingsString: Imputing {missing_before} "
                    f"missing values with default value: '{self._default_value}'"
                )
                result = data.fillna(self._default_value)
            else:
                logger.error(
                    f"TransformerImputeMissingsString: Unknown strategy '{self._strategy}'"
                )
                raise ValueError(f"Unknown imputation strategy: {self._strategy}")

            missing_after = result[string_cols].isnull().sum().sum()
            imputed_count = missing_before - missing_after

            logger.info(
                f"TransformerImputeMissingsString: Successfully imputed {imputed_count} "
                f"values ({missing_after} missing values remaining)"
            )

            return result

        except Exception:
            logger.exception(
                f"TransformerImputeMissingsString: Error during imputation with "
                f"strategy '{self._strategy}'"
            )
            raise

class TransformerFilterRows(Transformer):
    """
    Transformer that filters rows based on a condition.

    Supports filtering by column values using various operators or
    a custom callable condition.
    """

    def __init__(
        self,
        column: str = None,
        operator: str = None,
        value: Any = None,
        condition: Callable[[pd.DataFrame], pd.Series] = None
    ):
        """
        Initialize the row filter transformer.

        :param column: Column name to filter on (required if using operator)
        :type column: str
        :param operator: Comparison operator ('==', '!=', '>', '<', '>=', '<=', 'in', 'not_in', 'contains')
        :type operator: str
        :param value: Value to compare against
        :type value: Any
        :param condition: Custom callable that takes DataFrame and returns boolean Series
        :type condition: Callable[[pd.DataFrame], pd.Series]
        :raises ValueError: If neither operator nor condition is provided
        """
        self._column = column
        self._operator = operator
        self._value = value
        self._condition = condition

        logger.debug(
            f"TransformerFilterRows initialized with column='{column}', "
            f"operator='{operator}', value={value}, "
            f"custom_condition={condition is not None}"
        )

        if condition is None and operator is None:
            logger.error("TransformerFilterRows: Must provide either operator or condition")
            raise ValueError("Must provide either operator or condition")

        if operator is not None and column is None:
            logger.error("TransformerFilterRows: Column must be specified when using operator")
            raise ValueError("Column must be specified when using operator")

        valid_operators = ['==', '!=', '>', '<', '>=', '<=', 'in', 'not_in', 'contains']
        if operator is not None and operator not in valid_operators:
            logger.error(
                f"TransformerFilterRows: Invalid operator '{operator}'. "
                f"Valid options: {valid_operators}"
            )
            raise ValueError(f"Invalid operator. Valid options: {valid_operators}")
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Filter rows based on the specified condition.

        :param data: DataFrame to filter
        :type data: pd.DataFrame
        :return: Filtered DataFrame
        :rtype: pd.DataFrame
        :raises ValueError: If column doesn't exist or condition fails
        """
        initial_rows = len(data)
        logger.debug(f"TransformerFilterRows: Processing DataFrame with shape {data.shape}")

        try:
            if self._condition is not None:
                logger.info("TransformerFilterRows: Applying custom condition")
                mask = self._condition(data)
            else:
                if self._column not in data.columns:
                    logger.error(f"TransformerFilterRows: Column '{self._column}' not found")
                    raise ValueError(f"Column '{self._column}' not found in DataFrame")

                logger.info(
                    f"TransformerFilterRows: Filtering column '{self._column}' "
                    f"with operator '{self._operator}'"
                )

                if self._operator == '==':
                    mask = data[self._column] == self._value
                elif self._operator == '!=':
                    mask = data[self._column] != self._value
                elif self._operator == '>':
                    mask = data[self._column] > self._value
                elif self._operator == '<':
                    mask = data[self._column] < self._value
                elif self._operator == '>=':
                    mask = data[self._column] >= self._value
                elif self._operator == '<=':
                    mask = data[self._column] <= self._value
                elif self._operator == 'in':
                    mask = data[self._column].isin(self._value)
                elif self._operator == 'not_in':
                    mask = ~data[self._column].isin(self._value)
                elif self._operator == 'contains':
                    mask = data[self._column].str.contains(self._value, na=False)

            result = data[mask]
            removed_rows = initial_rows - len(result)

            logger.info(
                f"TransformerFilterRows: Kept {len(result)} rows, "
                f"removed {removed_rows} rows ({removed_rows/initial_rows*100:.2f}%)"
            )
            logger.debug(f"TransformerFilterRows: Output shape {result.shape}")

            return result

        except Exception:
            logger.exception("TransformerFilterRows: Error during filtering")
            raise

class TransformerSelectColumns(Transformer):
    """
    Transformer that selects specific columns from a DataFrame.

    Can select columns by name, drop columns, or reorder columns.
    """

    def __init__(
        self,
        columns: List[str] = None,
        drop: List[str] = None,
        keep_order: bool = True
    ):
        """
        Initialize the column selection transformer.

        :param columns: List of column names to keep (mutually exclusive with drop)
        :type columns: List[str]
        :param drop: List of column names to drop (mutually exclusive with columns)
        :type drop: List[str]
        :param keep_order: Whether to maintain column order from columns list
        :type keep_order: bool
        :raises ValueError: If both columns and drop are specified
        """
        self._columns = columns
        self._drop = drop
        self._keep_order = keep_order

        logger.debug(
            f"TransformerSelectColumns initialized with columns={columns}, "
            f"drop={drop}, keep_order={keep_order}"
        )

        if columns is not None and drop is not None:
            logger.error("TransformerSelectColumns: Cannot specify both columns and drop")
            raise ValueError("Cannot specify both 'columns' and 'drop' parameters")

        if columns is None and drop is None:
            logger.error("TransformerSelectColumns: Must specify either columns or drop")
            raise ValueError("Must specify either 'columns' or 'drop' parameter")

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Select or drop specified columns.

        :param data: DataFrame to process
        :type data: pd.DataFrame
        :return: DataFrame with selected columns
        :rtype: pd.DataFrame
        :raises ValueError: If specified columns don't exist
        """
        logger.debug(f"TransformerSelectColumns: Processing DataFrame with shape {data.shape}")
        logger.debug(f"TransformerSelectColumns: Input columns: {list(data.columns)}")

        try:
            if self._columns is not None:
                # Validate all columns exist
                missing_cols = set(self._columns) - set(data.columns)
                if missing_cols:
                    logger.error(
                        f"TransformerSelectColumns: Columns not found: {missing_cols}"
                    )
                    raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

                logger.info(
                    f"TransformerSelectColumns: Selecting {len(self._columns)} columns: "
                    f"{self._columns}"
                )
                result = data[self._columns]

            else:  # self._drop is not None
                # Validate columns to drop exist
                missing_cols = set(self._drop) - set(data.columns)
                if missing_cols:
                    logger.warning(
                        f"TransformerSelectColumns: Columns to drop not found: {missing_cols}"
                    )

                logger.info(
                    f"TransformerSelectColumns: Dropping {len(self._drop)} columns: "
                    f"{self._drop}"
                )
                result = data.drop(columns=self._drop, errors='ignore')

            logger.info(
                f"TransformerSelectColumns: Output has {len(result.columns)} columns"
            )
            logger.debug(f"TransformerSelectColumns: Output columns: {list(result.columns)}")
            logger.debug(f"TransformerSelectColumns: Output shape {result.shape}")

            return result

        except Exception:
            logger.exception("TransformerSelectColumns: Error during column selection")
            raise

class TransformerGroupByAggregate(Transformer):
    """
    Transformer that performs group-by aggregations.
    
    Groups data by specified columns and applies aggregation functions.
    """

    def __init__(
        self,
        group_by: Union[str, List[str]],
        aggregations: Dict[str, Union[str, List[str]]],
        reset_index: bool = True
    ):
        """
        Initialize the group-by aggregation transformer.

        :param group_by: Column(s) to group by
        :type group_by: Union[str, List[str]]
        :param aggregations: Dict mapping column names to aggregation function(s)
                            Example: {'sales': 'sum', 'price': ['mean', 'max']}
        :type aggregations: Dict[str, Union[str, List[str]]]
        :param reset_index: Whether to reset index after groupby
        :type reset_index: bool
        """
        self._group_by = [group_by] if isinstance(group_by, str) else group_by
        self._aggregations = aggregations
        self._reset_index = reset_index

        logger.debug(
            f"TransformerGroupByAggregate initialized with group_by={self._group_by}, "
            f"aggregations={aggregations}, reset_index={reset_index}"
        )

        valid_agg_funcs = ['sum', 'mean', 'median', 'min', 'max', 'count', 'std', 'var', 'first', 'last']

        # Validate aggregation functions
        for col, funcs in aggregations.items():
            func_list = [funcs] if isinstance(funcs, str) else funcs
            invalid_funcs = set(func_list) - set(valid_agg_funcs)
            if invalid_funcs:
                logger.warning(
                    f"TransformerGroupByAggregate: Potentially invalid aggregation "
                    f"functions for column '{col}': {invalid_funcs}"
                )

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Perform group-by aggregation.

        :param data: DataFrame to aggregate
        :type data: pd.DataFrame
        :return: Aggregated DataFrame
        :rtype: pd.DataFrame
        :raises ValueError: If group_by columns or aggregation columns don't exist
        """
        initial_rows = len(data)
        logger.debug(f"TransformerGroupByAggregate: Processing DataFrame with shape {data.shape}")

        try:
            # Validate group_by columns exist
            missing_group_cols = set(self._group_by) - set(data.columns)
            if missing_group_cols:
                logger.error(
                    f"TransformerGroupByAggregate: Group-by columns not found: {missing_group_cols}"
                )
                raise ValueError(f"Group-by columns not found: {missing_group_cols}")

            # Validate aggregation columns exist
            missing_agg_cols = set(self._aggregations.keys()) - set(data.columns)
            if missing_agg_cols:
                logger.error(
                    f"TransformerGroupByAggregate: Aggregation columns not found: {missing_agg_cols}"
                )
                raise ValueError(f"Aggregation columns not found: {missing_agg_cols}")

            logger.info(
                f"TransformerGroupByAggregate: Grouping by {self._group_by} "
                f"and aggregating {len(self._aggregations)} columns"
            )

            result = data.groupby(self._group_by).agg(self._aggregations)

            if self._reset_index:
                logger.debug("TransformerGroupByAggregate: Resetting index")
                result = result.reset_index()

            # Flatten column names if multi-level
            if isinstance(result.columns, pd.MultiIndex):
                logger.debug("TransformerGroupByAggregate: Flattening multi-level columns")
                result.columns = ['_'.join(col).strip('_') for col in result.columns.values]

            logger.info(
                f"TransformerGroupByAggregate: Reduced from {initial_rows} rows "
                f"to {len(result)} groups"
            )
            logger.debug(f"TransformerGroupByAggregate: Output shape {result.shape}")
            logger.debug(f"TransformerGroupByAggregate: Output columns: {list(result.columns)}")

            return result

        except Exception:
            logger.exception("TransformerGroupByAggregate: Error during aggregation")
            raise
