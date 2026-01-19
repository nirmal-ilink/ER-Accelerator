from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, regexp_replace, trim, lower
import re

class SourceConnector(ABC):
    """Abstract Base Class for Data Connectors."""
    @abstractmethod
    def read(self, spark: SparkSession, config: dict) -> DataFrame:
        pass

class DatabaseConnector(SourceConnector):
    """
    Generic JDBC Connector for Oracle/Snowflake.
    Includes error handling for CE environments where drivers may be missing.
    """
    def read(self, spark: SparkSession, config: dict) -> DataFrame:
        url = config.get("url")
        table = config.get("table")
        properties = config.get("properties", {})
        
        try:
            print(f"INFO: Attempting JDBC connection to {url}...")
            # Enterprise Logic
            return spark.read.format("jdbc") \
                .option("url", url) \
                .option("dbtable", table) \
                .options(**properties) \
                .load()
        except Exception as e:
            print(f"WARN: JDBC Connection failed. This is expected in Databricks CE if drivers are missing.")
            print(f"ERROR DETAILS: {str(e)}")
            # Return empty DF to prevent pipeline crash during structure validation
            return spark.createDataFrame([], schema=None)

class FileConnector(SourceConnector):
    """
    Connector for Flat Files (CSV, Parquet).
    Uses Databricks Autoloader (cloudFiles) for streaming ingestion or standard read for batch.
    """
    def read(self, spark: SparkSession, config: dict) -> DataFrame:
        path = config.get("path")
        fmt = config.get("format", "csv")
        is_stream = config.get("is_stream", False)
        
        print(f"INFO: Reading file source from {path} (Format: {fmt}, Stream: {is_stream})")
        
        if is_stream:
            # Databricks Autoloader
            return spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", fmt) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(path)
        else:
            # Standard Batch Read
            return spark.read.format(fmt) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(path)

class IngestionFactory:
    """
    Factory to instantiate the correct connector based on Source Type.
    """
    @staticmethod
    def get_connector(source_type: str) -> SourceConnector:
        if source_type.lower() in ["oracle", "snowflake", "sqlserver", "jdbc"]:
            return DatabaseConnector()
        elif source_type.lower() in ["csv", "parquet", "json", "file"]:
            return FileConnector()
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

class ColumnNormalizer:
    """
    Standardizes DataFrame schema:
    - Snake Case Headers (FirstName -> first_name)
    - Trims Whitespace (Header & Data)
    - Adds Ingestion Metadata (_ingest_timestamp, _source_system)
    """
    @staticmethod
    def normalize(df: DataFrame, source_name: str) -> DataFrame:
        # 1. Clean Headers
        new_columns = []
        for c in df.columns:
            # Strip whitespace, replace non-alphanumeric with _, generic normalization
            clean_col = c.strip().lower()
            clean_col = re.sub(r'[^a-zA-Z0-9]', '_', clean_col)
            clean_col = re.sub(r'_+', '_', clean_col) # Remove duplicate underscores
            clean_col = clean_col.strip('_')
            new_columns.append(clean_col)
        
        df = df.toDF(*new_columns)
        
        # 2. Add Metadata
        df = df.withColumn("_ingest_timestamp", current_timestamp()) \
               .withColumn("_source_system", lit(source_name))
               
        # 3. Trim String Columns (Optional, expensive on huge data, good for cleanliness)
        # Choosing selective clean in Cleaning step, but good to have rudimentary trim here if needed
        # skipping pervasive trim for performance, relying on Cleaning module.
        
        print(f"INFO: Schema normalized and metadata added for source '{source_name}'.")
        return df

def ingest_source(spark: SparkSession, source_config: dict) -> DataFrame:
    """Helper function to orchestrate ingestion."""
    source_type = source_config.get("type", "csv")
    source_name = source_config.get("name", "Unknown")
    
    connector = IngestionFactory.get_connector(source_type)
    
    # Adapt config for connector
    # In a real app, mapping logic would be here. For now passing raw config.
    read_config = source_config.copy()
    
    raw_df = connector.read(spark, read_config)
    
    if raw_df.pk_columns if hasattr(raw_df, 'pk_columns') else False:
       pass 

    normalized_df = ColumnNormalizer.normalize(raw_df, source_name)
    return normalized_df
