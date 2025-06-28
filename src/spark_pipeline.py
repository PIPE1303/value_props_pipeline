"""
PySpark implementation of the Value Props Ranking Pipeline.
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, sum as spark_sum, count as spark_count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
import logging
from datetime import timedelta
from typing import Optional
from .config import SPARK_CONFIG, WINDOW_DAYS, RECENT_DAYS, FINAL_COLUMNS

logger = logging.getLogger(__name__)

class SparkValuePropsPipeline:
    """PySpark implementation of the Value Props Ranking Pipeline."""
    
    def __init__(self):
        """Initialize Spark session and pipeline."""
        self.spark = self._create_spark_session()
        logger.info("Spark session initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        builder = SparkSession.builder
        
        for key, value in SPARK_CONFIG.items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def load_data(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        """
        Load prints, taps, and pays data into Spark DataFrames.
        
        Returns:
            tuple: (prints_df, taps_df, pays_df)
        """
        logger.info("Loading data into Spark...")
        
        prints_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("value_prop_id", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        taps_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("value_prop_id", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        pays_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("value_prop_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("amount", DoubleType(), True)
        ])
        
        prints_df = self.spark.read.json("data/prints.json", schema=prints_schema)
        taps_df = self.spark.read.json("data/taps.json", schema=taps_schema)
        pays_df = self.spark.read.csv("data/pays.csv", header=True, schema=pays_schema)
        
        logger.info(f"Data loaded: {prints_df.count()} prints, {taps_df.count()} taps, {pays_df.count()} pays")
        return prints_df, taps_df, pays_df
    
    def add_click_flag(self, prints_df: DataFrame, taps_df: DataFrame) -> DataFrame:
        """
        Add 'clicked' column based on matches with taps.
        
        Args:
            prints_df: Prints DataFrame
            taps_df: Taps DataFrame
            
        Returns:
            DataFrame: DataFrame with 'clicked' column
        """
        logger.info("Adding click flag...")
        
        taps_unique = taps_df.select("user_id", "value_prop_id", "timestamp").distinct()
        
        result = prints_df.join(
            taps_unique,
            on=["user_id", "value_prop_id", "timestamp"],
            how="left"
        )
        
        result = result.withColumn("clicked", when(col("user_id").isNotNull(), 1).otherwise(0))
        
        click_count = result.filter(col("clicked") == 1).count()
        logger.info(f"Click flag added. Clicks found: {click_count}")
        
        return result
    
    def add_historical_features(
        self,
        df: DataFrame,
        source_df: DataFrame,
        window_start: str,
        window_end: str,
        feature_name: str,
        agg_type: str = "count"
    ) -> DataFrame:
        """
        Add historical features based on time windows.
        
        Args:
            df: Main DataFrame
            source_df: Source DataFrame for feature calculation
            window_start: Start of time window
            window_end: End of time window
            feature_name: Name of the feature to create
            agg_type: Aggregation type (count, sum, mean)
            
        Returns:
            DataFrame: DataFrame with historical feature added
        """
        logger.info(f"Adding historical feature: {feature_name}")
        
        filtered_source = source_df.filter(
            (col("timestamp") >= window_start) & (col("timestamp") < window_end)
        )
        
        if agg_type == "count":
            feature_df = filtered_source.groupBy("user_id", "value_prop_id").agg(
                spark_count("*").alias(feature_name)
            )
        elif agg_type == "sum":
            feature_df = filtered_source.groupBy("user_id", "value_prop_id").agg(
                spark_sum("amount").alias(feature_name)
            )
        else:
            raise ValueError(f"Unsupported aggregation type: {agg_type}")
        
        result = df.join(feature_df, on=["user_id", "value_prop_id"], how="left")
        result = result.na.fill(0, subset=[feature_name])
        
        logger.info(f"Historical feature '{feature_name}' added")
        return result
    
    def create_features_pipeline(
        self,
        prints_df: DataFrame,
        taps_df: DataFrame,
        pays_df: DataFrame,
        end_date: Optional[str] = None
    ) -> DataFrame:
        """
        Complete feature creation pipeline.
        
        Args:
            prints_df: Prints DataFrame
            taps_df: Taps DataFrame
            pays_df: Pays DataFrame
            end_date: End date for analysis
            
        Returns:
            DataFrame: DataFrame with all features
        """
        logger.info("Starting Spark features pipeline...")
        
        if end_date is None:
            end_date = prints_df.agg({"timestamp": "max"}).collect()[0][0]
        
        start_last_week = end_date - timedelta(days=RECENT_DAYS)
        start_3weeks_ago = end_date - timedelta(days=WINDOW_DAYS)
        
        logger.info(f"Analysis window: {start_last_week} to {end_date}")
        logger.info(f"Historical window: {start_3weeks_ago} to {start_last_week}")
        
        recent_prints = prints_df.filter(
            (col("timestamp") >= start_last_week) & (col("timestamp") <= end_date)
        )
        
        recent_count = recent_prints.count()
        logger.info(f"Recent prints found: {recent_count}")
        
        recent_prints = self.add_click_flag(recent_prints, taps_df)
        
        recent_prints = self.add_historical_features(
            recent_prints, prints_df, start_3weeks_ago, start_last_week,
            "print_count_3w", "count"
        )
        
        recent_prints = self.add_historical_features(
            recent_prints, taps_df, start_3weeks_ago, start_last_week,
            "tap_count_3w", "count"
        )
        
        recent_prints = self.add_historical_features(
            recent_prints, pays_df, start_3weeks_ago, start_last_week,
            "pay_count_3w", "count"
        )
        
        recent_prints = self.add_historical_features(
            recent_prints, pays_df, start_3weeks_ago, start_last_week,
            "total_amount_3w", "sum"
        )
        
        logger.info("Spark features pipeline completed successfully")
        return recent_prints
    
    def run_pipeline(self, output_filename: str = "dataset_final_spark.csv") -> DataFrame:
        """
        Execute the complete Spark pipeline.
        
        Args:
            output_filename: Output filename
            
        Returns:
            DataFrame: Final dataset
        """
        logger.info("Executing Spark pipeline...")
        
        try:
            prints_df, taps_df, pays_df = self.load_data()
            
            dataset = self.create_features_pipeline(prints_df, taps_df, pays_df)
            
            final_dataset = dataset.select(FINAL_COLUMNS)
            
            final_dataset.write.mode("overwrite").csv(
                f"output/{output_filename}",
                header=True
            )
            
            logger.info("Spark pipeline completed successfully")
            return final_dataset
            
        except Exception as e:
            logger.error(f"Error in Spark pipeline: {str(e)}")
            raise
    
    def __del__(self):
        """Clean up Spark session."""
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped") 