import logging
from datetime import datetime
from telefonipy.dataman import run_sql_file, get_df_from_query

class Table:
    """
    A class to manage and interact with database tables from different data engines.

    Attributes:
        name (str): The name of the table.
        schema (str): The schema to which the table belongs.
        dsn (str): The data source name ('DWH_TERADATA' or 'BIGDATA_CDP').
        sql_name (str): Fully qualified table name (schema.name).
        motor (str): Database engine ('teradata' for 'DWH_TERADATA', 'hive' for 'BIGDATA_CDP').
        period_column (str): Column used for tracking periods.
        max_period (str): Maximum period value in the table.
        min_period (str): Minimum period value in the table.
        date_column (Optional[str]): Column used for tracking dates.
        max_date (Optional[str]): Maximum date value in the table.
        min_date (Optional[str]): Minimum date value in the table.
    """

    def __init__(self, name: str, schema: str, dsn: str, period_column: str = None):
        """
        Initialize a Table instance with name, schema, and DSN.

        Args:
            name (str): The table name.
            schema (str): The schema name.
            dsn (str): Data source name, either 'DWH_TERADATA' or 'BIGDATA_CDP'.

        Raises:
            TypeError: If arguments are not of string type.
            ValueError: If DSN is invalid.
        """
        if not all(isinstance(arg, str) for arg in [name, schema, dsn]):
            raise TypeError("All arguments must be of string type.")
        if dsn not in ['DWH_TERADATA', 'BIGDATA_CDP']:
            raise ValueError("dsn parameter should be 'DWH_TERADATA' or 'BIGDATA_CDP'.")

        self.name = name.strip()
        self.schema = schema.strip()
        self.dsn = dsn
        self.sql_name = f"{self.schema}.{self.name}"
        self.motor = 'teradata' if dsn == 'DWH_TERADATA' else 'hive'
        self.period_column = period_column
        self.max_period = None
        self.min_period = None
        self.date_column = None
        self.max_date = None
        self.min_date = None

    def __repr__(self):
        """
        String representation of the Table instance.

        Returns:
            str: A string containing table details.
        """
        return (f"Table(name='{self.name}', schema='{self.schema}', dsn='{self.dsn}', "
                f"sql_name='{self.sql_name}', motor='{self.motor}', "
                f"max_period={self.max_period}, min_period={self.min_period}, "
                f"max_date={self.max_date}, min_date={self.min_date})")
    
    def query_data(self, query: str):
        """
        Run a query and return the result as a DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            DataFrame: Query results.

        Raises:
            TypeError: If query is not a string.
        """
        if not isinstance(query, str):
            raise TypeError("The 'query' argument must be a string.")
        logging.info(f"Running query on {self.sql_name}: {query}")
        return get_df_from_query(query, self.dsn)

    def update_periods(self, period_column: str):
        """
        Update the max and min periods from the table based on the specified column.

        Args:
            period_column (str): The name of the period column.

        Raises:
            TypeError: If period_column is not a string.
            ValueError: If the query returns no results or period_column is invalid.
        """
        if not isinstance(period_column, str):
            raise TypeError("'period_column' must be a string.")
        self.period_column = period_column

        query = f"SELECT MAX({period_column}), MIN({period_column}) FROM {self.sql_name}"
        df = self.query_data(query)
        if df.empty:
            raise ValueError(f"No data found for column '{period_column}' in table '{self.sql_name}'.")

        self.max_period = str(df.iloc[0, 0])
        self.min_period = str(df.iloc[0, 1])
        logging.info(f"Updated periods for {self.sql_name}: max_period={self.max_period}, min_period={self.min_period}")

    def update_dates(self, date_column: str):
        if not isinstance(date_column, str):
            raise TypeError("'date_column' must be a string.")
        self.date_column = date_column

        query = f"SELECT MAX({date_column}), MIN({date_column}) FROM {self.sql_name}"
        df = self.query_data(query)
        if df.empty:
            raise ValueError(f"No data found for column '{date_column}' in table '{self.sql_name}'.")

        self.max_date = str(df.iloc[0, 0])
        self.min_date = str(df.iloc[0, 1])
        logging.info(f"Updated dates for {self.sql_name}: max_date={self.max_date}, min_date={self.min_date}")

    def compare_period(self, comparison_period: str, greater: bool = True, max: bool = True) -> bool:
        """
        Compare a given period against the max or min period in the table.

        Args:
            comparison_period (str): The date string to compare in 'YYYY-MM-DD' format.
            greater (bool, optional): Whether to check if the table's period is greater. Defaults to True.
            max (bool, optional): Whether to use the max period (if False, uses min). Defaults to True.

        Returns:
            bool: Comparison result.

        Raises:
            ValueError: If the comparison_period format is invalid, or periods hasn't been set.
        """
        try:
            comparison_period = datetime.strptime(comparison_period, r"%Y-%m-%d").date()
        except ValueError:
            raise ValueError("comparison_period must be in 'YYYY-MM-DD' format.")

        table_period_str = self.max_period if max else self.min_period
        if not table_period_str:
            raise ValueError(f"The {'max' if max else 'min'} period is not set. Please run 'update_periods' first.")

        try:
            table_period = datetime.strptime(table_period_str, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError(f"{'max' if max else 'min'} period must be in 'YYYY-MM-DD' format.")
        result = comparison_period > table_period if greater else table_period > comparison_period
        logging.info(f"Comparison result for {comparison_period}: {result}")
        return result

    def compare_date(self, comparison_date: str, greater: bool = True, max: bool = True) -> bool:
        try:
            comparison_date = datetime.strptime(comparison_date, r"%Y-%m-%d").date()
        except ValueError:
            raise ValueError("comparison_date must be in 'YYYY-MM-DD' format.")

        table_date_str = self.max_date if max else self.min_date
        if not table_date_str:
            raise ValueError(f"The {'max' if max else 'min'} date is not set. Please run 'update_dates' first.")

        table_date = datetime.strptime(table_date_str, "%Y-%m-%d").date()
        result = comparison_date > table_date if greater else table_date > comparison_date
        logging.info(f"Comparison result for {comparison_date}: {result}")
        return result

    def get_period_datetime(self, max=True, date=True):
        """
        Retrieve the maximum or minimum period as either a date or datetime object.

        Args:
            max (bool): If True, returns the maximum period. If False, returns the minimum period.
            date (bool): If True, returns a `datetime.date` object. If False, returns a `datetime.datetime` object.

        Returns:
            datetime.date or datetime.datetime: The selected period as a `datetime.date` or `datetime.datetime` object.

        Raises:
            ValueError: If the selected period (max or min) is not set or is not in the expected format.
        """
        # Select the appropriate period
        period = self.max_period if max else self.min_period

        if not period:
            raise ValueError(f"The {'max' if max else 'min'} period is not set. Please run 'update_periods' first.")

        try:
            # Parse the period into a datetime object
            period = datetime.strptime(period, r"%Y-%m-%d")
        except ValueError:
            raise ValueError("The period must be in 'YYYY-MM-DD' format.")

        # Return the period as either a date or datetime object
        return period.date() if date else period

    def run_modification_script(self, file: str, silent: bool = False, params: dict = None):
        """
        Run a SQL script file to modify the table.

        Args:
            file (str): The file path to the SQL script.
            silent (bool, optional): Whether to suppress logs. Defaults to False.
            params (dict, optional): Parameters for the SQL script. Defaults to None.

        Returns:
            Any: Result of the SQL script execution.

        Raises:
            TypeError: If file is not a string or params is not a dictionary.
        """
        if not isinstance(file, str):
            raise TypeError("'file' argument must be a string.")
        if params is not None and not isinstance(params, dict):
            raise TypeError("'params' argument must be a dictionary.")
        
        logging.info(f"Running modification script '{file}' on {self.sql_name}.")
        return run_sql_file(file, self.dsn, silent=silent, params=params)
    
    def insert_if(self, period, file, silent=None, params=None):
        """
        Run an update script if the given period is smaller than the maximum period in the table.

        Args:
            period (str): The period to add to the table in 'YYYY-MM-DD' format.
            file (str): The file path to the SQL update script.
            silent (bool, optional): If True, suppresses logs of the SQL execution. Defaults to None.
            params (dict, optional): Parameters to pass to the SQL script. Defaults to None.

        Returns:
            bool: True if the script was executed, False otherwise.
        
        Raise:
            TypeError: If period argument is not a string.
            ValueError: If period argument is not in YYYY-MM-DD format.
        """
        if not isinstance(period, str):
            raise TypeError("'period' argument should be a string")

        # Validate date format
        try:
            datetime.strptime(period, r"%Y-%m-%d").date()
        except ValueError:
            raise ValueError("The 'period' must be in 'YYYY-MM-DD' format.")

        # Check if the insertion is required
        if self.compare_period(period):
            logging.info(f"Insertion of period {period} in table {self.name} is required.")
            
            # Run the modification script
            self.run_modification_script(file, silent=silent, params=params)
            return True
        else:
            logging.info(f"Insertion of period {period} in table {self.name} is NOT required.")
            return False

    def delete_if(self, period, file, silent=None, params=None):
        """
        Run a deletion script if the given period is greater than the minimum period in the table.

        Args:
            period (str): The period to compare in 'YYYY-MM-DD' format.
            file (str): The file path to the SQL deletion script.
            silent (bool, optional): If True, suppresses logs of the SQL execution. Defaults to None.
            params (dict, optional): Parameters to pass to the SQL script. Defaults to None.

        Returns:
            bool: True if the script was executed, False otherwise.
        """
        if not self.period_column:
            raise ValueError("The 'period_column' is not set. Update periods using the 'update_periods' method first.")

        if not isinstance(period, str):
            raise TypeError("'period' argument should be a string")

        # Validate date format
        try:
            datetime.strptime(period, r"%Y-%m-%d").date()
        except ValueError:
            raise ValueError("The 'period' must be in 'YYYY-MM-DD' format.")

        # Check if the deletion is required
        if self.compare_period(period, greater=True, max=False):
            logging.info(f"Deletion of period {self.min_period} in table {self.name} is required.")
            
            # Run the modification script
            self.run_modification_script(file, silent=silent, params=params)
            return True
        else:
            logging.info(f"Deletion of period {self.min_period} in table {self.name} is NOT required.")
            return False

    def get_df(self):
        """
        Retrieve the entire table as a pandas DataFrame.

        This method runs a query to fetch all rows and columns from the table 
        without applying any filters.

        Args:
            None

        Returns:
            pandas.DataFrame: A DataFrame representing the entire table.
        """
        query = f"""
            SELECT
                *
            FROM {self.sql_name}
        """
        return get_df_from_query(query, self.dsn)

    def delete_period(self, period, deletion_file_path, silent=False):
        params_dict = {
            "name": f"{self.sql_name}",
            "column": f"{self.period_column}",
            "mes": f"'{period}'"
        }

        return self.run_modification_script(deletion_file_path, silent=silent, params=params_dict)








        





