from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd
import re

class DataCleaner:
    """
    High-Performance Vectorized Data Cleaning using Pandas UDFs.
    Cleaners operate on pandas Series (Apache Arrow) for speed.
    """

    @staticmethod
    @pandas_udf(StringType())
    def clean_text_udf(series: pd.Series) -> pd.Series:
        """
        Standard text cleaner:
        - Lowercase
        - Remove special characters (keep alphanumeric and space)
        - Collapse multiple spaces
        - Trim
        """
        def clean(s):
            if s is None:
                return None
            s = str(s).lower()
            s = re.sub(r'[^a-z0-9\s]', '', s) # Remove special chars
            s = re.sub(r'\s+', ' ', s)        # Collapse whitespace
            return s.strip()

        return series.apply(clean)

    @staticmethod
    @pandas_udf(StringType())
    def clean_phone_udf(series: pd.Series) -> pd.Series:
        """
        Extracts digits only from phone numbers.
        """
        def clean(s):
            if s is None:
                return None
            s = str(s)
            digits = re.sub(r'\D', '', s)
            return digits if digits else None
            
        return series.apply(clean)
    
    @staticmethod
    @pandas_udf(StringType())
    def generate_soundex_udf(series: pd.Series) -> pd.Series:
        """
        Vectorized Soundex generation (using simple python implementation applied via pandas).
        For production, prefer Spark's native 'soundex' function, but this shows UDF flexibility.
        """
        # Note: Spark has built-in soundex(), metaphone is usually external.
        # This is a placeholder for more complex phonetic algos like Metaphone/Double Metaphone 
        # which aren't built-in to Spark SQL.
        
        def get_soundex(token):
            if not token: return None
            # Simple placeholder logic or import Jellyfish here
            token = token.upper()
            return token[0] # Simplistic just to demo structure
            
        return series.apply(get_soundex)

    @staticmethod
    def apply_cleaning_rules(df, rules: dict):
        """
        Applies a dictionary of cleaning rules to a DataFrame.
        rules = {'col_name': 'method_name'}
        """
        for col_name, method in rules.items():
            if method == 'clean_text':
                df = df.withColumn(col_name, DataCleaner.clean_text_udf(df[col_name]))
            elif method == 'clean_phone':
                df = df.withColumn(col_name, DataCleaner.clean_phone_udf(df[col_name]))
        return df
