import logging
from pathlib import Path
import json
from typing import Dict, Any, List
from pandas import DataFrame


class Process:
    def __init__(self, base_path: Path, logger: logging.Logger = None, log_file: str = None):
        """
        Initialize the Process class.

        :param base_path: Path to the base directory for process-related files.
        :param logger: Pre-configured logger instance. If None, a default logger is created.
        :param log_file: Optional file path to save logs.
        """
        if not isinstance(base_path, Path):
            raise ValueError("base_path must be an instance of pathlib.Path")
        
        self.base_path = base_path
        self.parameters = {}
        self.dsn_dwh = None
        self.dsn_hive = None

        # Use the provided logger or create a new one
        self.logger = logger or logging.getLogger("MAIN")

        if not logger:
            self.logger.setLevel(logging.INFO)
            self.logger.info("No logger provided; using default logger.")

        # Add a FileHandler if log_file is specified
        if log_file:
            self._add_file_handler(log_file)

    def _add_file_handler(self, log_file: str) -> None:
        """
        Adds a file handler to the logger.

        :param log_file: File path where logs will be saved.
        """
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Set the logging level for the file
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)
        self.logger.info("File handler added to logger: %s", log_file)

    def load_config(self) -> None:
        try:
            params_path = self.base_path / 'parameters_proc.json'
            self.parameters = self._load_json(params_path)

            self.dsn_dwh = self.parameters.get('dsn_dwh', 'DWH_TERADATA')
            self.dsn_hive = self.parameters.get('dsn_hive', 'BIGDATA_CDP')

            self.logger.info("Configuration loaded successfully")
        except FileNotFoundError:
            self.logger.error(f"Parameters file not found at {params_path}")
            raise
        except KeyError as e:
            self.logger.error(f"Missing required parameter: {e}")
            raise        

    @staticmethod
    def _load_json(file_path: Path) -> Dict[str, Any]:
        """
        Load and parse a JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Dict containing the parsed JSON data
            
        Raises:
            JSONDecodeError: If JSON is malformed
            FileNotFoundError: If file doesn't exist
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {file_path}: {str(e)}")
        
    @staticmethod
    def df_size(df: DataFrame) -> float:
        """
        Calculate the memory size of a DataFrame in megabytes.
        
        Args:
            df: pandas DataFrame to measure
            
        Returns:
            float: Size of the DataFrame in MB
            
        Example:
            >>> df = pd.DataFrame({'col1': range(1000)})
            >>> Process._df_size(df)
            0.015625  # Size in MB
        """
        try:
            # Include index in memory calculation
            size_mb = df.memory_usage(deep=True).sum() / 1024**2
            return size_mb
        except Exception as e:
            raise ValueError(f"Error calculating DataFrame size: {str(e)}")

    @staticmethod    
    def optimize_dtypes(df: DataFrame) -> DataFrame:
        """Optimize DataFrame memory usage by converting datatypes"""
        for col in df.columns:
            # Convert int64 to int32 or int16 if possible
            if df[col].dtype == 'int64':
                if df[col].min() >= -32768 and df[col].max() <= 32767:
                    df[col] = df[col].astype('int16')
                else:
                    df[col] = df[col].astype('int32')
            # Convert object columns to categories if cardinality is low
            elif df[col].dtype == 'object':
                if df[col].nunique() / len(df) < 0.5:  # If less than 50% unique values
                    df[col] = df[col].astype('category')
        return df
    
    @staticmethod
    def nulls_imputation(df: DataFrame, columns: List[str], value: Any = 0, 
                         max_factor: float = 2, mean_factor: float = 0.75) -> DataFrame:
        """Imputate nulls in dataframe providing different logics with memory optimization"""
        if not isinstance(df, DataFrame):
            raise TypeError("The `df` argument must be a pandas DataFrame object.")
        
        for column in columns:
            if column not in df.columns:
                raise ValueError(f"Column `{column}` does not exist in the DataFrame.")
            
            if isinstance(value, int):
                df[column] = df[column].fillna(value)
            elif value == 'max':
                max_val = df[column].max()
                df[column] = df[column].fillna(max_val * max_factor)
            elif value == 'mean':
                mean_val = df[column].mean()
                df[column] = df[column].fillna(mean_val * mean_factor)
            else:
                raise ValueError("The `value` argument should be an integer, 'max', or 'mean'.")
        return df
    
    def execute(self) -> None:
        """
        Main execution logic for the process.
        
        Raises:
            NotImplementedError: This method must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement execute() method")
    