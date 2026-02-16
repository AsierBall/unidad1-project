from typing import Protocol, Any
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
            logger.warning(
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
