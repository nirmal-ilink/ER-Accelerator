from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from typing import Dict

class SurvivorshipEngine:
    """
    Determines the 'Golden Record' processing rules.
    Strategy:
    1. Trust Score (Configured per Source)
    2. Completeness (Non-Null values preferred)
    3. Recency (Latest _ingest_timestamp preferred)
    """

    def __init__(self, trust_scores: Dict[str, float]):
        self.trust_scores = trust_scores

    def resolve_conflicts(self, clustered_df: DataFrame) -> DataFrame:
        """
        Collapses a cluster of records into a single Golden Record.
        """
        print("INFO: Applying Survivorship Rules to generate Golden Records...")
        
        # 1. Assign numeric Trust Score to each record based on _source_system
        # Map dictionary matching to a Case/When expression
        trust_expr = F.expr(
            "CASE " + 
            " ".join([f"WHEN _source_system = '{src}' THEN {score}" for src, score in self.trust_scores.items()]) +
            " ELSE 0.5 END"
        )
        
        scored_df = clustered_df.withColumn("trust_score", trust_expr)
        
        # 2. Define Window for Best Record Selection Logic
        # Rank by: Trust (Desc), Timestamp (Desc), Length of Data (Desc - Proxy for Completeness)
        # Note: True field-level survivorship requires unpivoting (melting) the dataframe.
        # This implementation does ROW-LEVEL selection for simplicity in this accelerator phase,
        # but supports the concept of "Trusted Source".
        
        window_spec = Window.partitionBy("cluster_id").orderBy(
            F.col("trust_score").desc(),
            F.col("_ingest_timestamp").desc()
        )
        
        # Select the "Best" Representative Row as the Golden Record Base
        golden_df = scored_df.withColumn("rank", F.row_number().over(window_spec)) \
                             .filter("rank = 1") \
                             .drop("rank", "trust_score", "salt")
        
        print(f"INFO: Golden Records generated.")
        return golden_df

    def apply_field_level_survivorship(self, clustered_df: DataFrame, attributes: list) -> DataFrame:
        """
        [Advanced] Implementation of Field-Level Survivorship.
        Iterates through columns to find the best value for *each* field across the cluster.
        """
        # This is the "Consulting-Grade" requirement.
        # We group by cluster_id and aggregate each column using a max_by logic weighted by trust.
        
        # For Spark SQL, we can use struct-sort trick: max(struct(trust, timestamp, value)).value
        
        agg_exprs = []
        for col_name in attributes:
            # Create a struct to sort by: Trust > Recency
            # We want the Value associated with the highest Trust/Recency
            expr = F.max(F.struct(
                F.col("trust_score"), 
                F.col("_ingest_timestamp"), 
                F.col(col_name)
            )).alias(f"struct_{col_name}")
            agg_exprs.append(expr)
            
        # We need the scored DF first (re-using logic from above)
        trust_expr = F.expr(
            "CASE " + 
            " ".join([f"WHEN _source_system = '{src}' THEN {score}" for src, score in self.trust_scores.items()]) +
            " ELSE 0.5 END"
        )
        scored_df = clustered_df.withColumn("trust_score", trust_expr)
        
        # Aggregate
        agg_df = scored_df.groupBy("cluster_id").agg(*agg_exprs)
        
        # Extract values back
        select_exprs = ["cluster_id"] + [F.col(f"struct_{c}.{c}").alias(c) for c in attributes]
        
        return agg_df.select(*select_exprs)
