from src.backend.bootstrapper import get_bootstrapper
from src.backend.engine.ingestion import ingest_source
from src.backend.engine.cleaning import DataCleaner
from src.backend.engine.matching import MatchingEngine
from src.backend.governance.survivorship import SurvivorshipEngine
from src.backend.audit.logger import AuditLogger
from pyspark.sql.functions import col

def run_healthcare_pipeline():
    # 1. Initialize Platform
    bs = get_bootstrapper()
    spark = bs.spark
    logger = AuditLogger(spark, bs.get_storage_path("system") + "events")
    
    logger.log_event("PIPELINE", "START", "RUNNING", {"pipeline": "healthcare_demo"})

    try:
        # 2. Ingest Sources
        # Config would ideally come from YAML, here hardcoded for demo flow
        emr_config = {"type": "csv", "path": bs.get_storage_path("landing") + "emr_data.csv", "name": "EMR_System_A"}
        npi_config = {"type": "csv", "path": bs.get_storage_path("landing") + "npi_registry.csv", "name": "NPI_Registry"}
        
        df_emr = ingest_source(spark, emr_config)
        df_npi = ingest_source(spark, npi_config)
        
        logger.log_event("INGESTION", "LOAD", "SUCCESS", {"emr_count": df_emr.count(), "npi_count": df_npi.count()})

        # 3. Clean & Normalize
        # Apply vector cleaning to names and addresses
        cleaning_rules = {
            "first_name": "clean_text", 
            "last_name": "clean_text", 
            "address_line_1": "clean_text",
            "phone": "clean_phone"
        }
        
        df_emr_clean = DataCleaner.apply_cleaning_rules(df_emr, cleaning_rules)
        df_npi_clean = DataCleaner.apply_cleaning_rules(df_npi, cleaning_rules)

        # Union for Matching
        # In waterfall, we typically match Reference against Input. Here taking Union approach.
        from functools import reduce
        from pyspark.sql import DataFrame
        
        # Align columns (simple approach: select common cols)
        common_cols = list(set(df_emr_clean.columns) & set(df_npi_clean.columns))
        df_combined = df_emr_clean.select(common_cols).unionByName(df_npi_clean.select(common_cols))
        
        logger.log_event("CLEANING", "NORMALIZE", "SUCCESS", {"total_records": df_combined.count()})

        # 4. Matching (Waterfall)
        matcher = MatchingEngine(spark)
        
        # Pass 1: Strict (Exact NPI)
        # Pass 2: Loose (Soundex Name + Zip) - simplifying to exact Zip + Name match for speed in demo
        waterfall_config = [
            {'name': 'Exact NPI Match', 'blocking_key': 'npi', 'method': 'exact'},
            {'name': 'Name + Zip Match', 'blocking_key': 'zip_code', 'method': 'exact'} # In reality, fuzzy on name, block on zip
        ]
        
        df_clustered = matcher.run_waterfall(df_combined, waterfall_config)
        
        match_count = df_clustered.filter(col("cluster_id").isNotNull()).count()
        logger.log_event("MATCHING", "WATERFALL", "SUCCESS", {"clustered_records": match_count})

        # 5. Governance (Golden Record)
        # Define Trust Scores
        trust_scores = {"NPI_Registry": 1.0, "EMR_System_A": 0.8}
        gov_engine = SurvivorshipEngine(trust_scores)
        
        df_gold = gov_engine.resolve_conflicts(df_clustered)
        
        logger.log_event("GOVERNANCE", "SURVIVORSHIP", "SUCCESS", {"golden_records": df_gold.count()})

        # 6. Export for Air-Gap Frontend
        output_path = bs.get_storage_path("local_output") + "gold_export.csv"
        # Convert to Pandas for local CSV write (Simulating the Air-Gap export)
        # In Prod: df_gold.write.format("delta").save(...)
        
        print(f"INFO: Exporting Golden Records to {output_path} for Streamlit...")
        df_gold.toPandas().to_csv(output_path, index=False)
        
        logger.log_event("PIPELINE", "COMPLETE", "SUCCESS")
        
    except Exception as e:
        logger.log_event("PIPELINE", "ERROR", "FAILED", {"error": str(e)})
        raise e

if __name__ == "__main__":
    run_healthcare_pipeline()
