"""
Pipeline de procesamiento usando PySpark para datasets grandes.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, count, sum as spark_sum, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, MapType, DateType
from datetime import timedelta
import logging
from typing import Optional
from .config import SPARK_CONFIG, PRINTS_FILE, TAPS_FILE, PAYS_FILE, WINDOW_DAYS, RECENT_DAYS, FINAL_COLUMNS

logger = logging.getLogger(__name__)

class SparkValuePropsPipeline:
    """Pipeline de procesamiento usando PySpark."""
    
    def __init__(self, spark_config: Optional[dict] = None):
        """
        Inicializa el pipeline de Spark.
        
        Args:
            spark_config: Configuración personalizada de Spark
        """
        self.spark_config = spark_config or SPARK_CONFIG
        self.spark = None
        self._setup_spark()
        self._define_schemas()
    
    def _setup_spark(self):
        """Configura la sesión de Spark."""
        logger.info("Configurando sesión de Spark...")
        
        builder = SparkSession.builder.appName(self.spark_config["app_name"])
        
        # Aplicar configuración personalizada
        for key, value in self.spark_config.items():
            if key != "app_name":
                builder = builder.config(key, value)
        
        self.spark = builder.getOrCreate()
        logger.info("Sesión de Spark configurada exitosamente")
    
    def _define_schemas(self):
        """Define los esquemas para los diferentes tipos de datos."""
        self.prints_schema = StructType([
            StructField("day", DateType()),
            StructField("event_data", MapType(StringType(), StringType())),
            StructField("user_id", StringType()),
        ])
        
        self.taps_schema = self.prints_schema
        
        self.pays_schema = StructType([
            StructField("pay_date", DateType()),
            StructField("amount", DoubleType()),
            StructField("user_id", StringType()),
            StructField("value_prop_id", StringType()),
        ])
    
    def load_data(self):
        """Carga los datos desde los archivos fuente."""
        logger.info("Cargando datos con Spark...")
        
        # Cargar prints
        prints = self.spark.read.schema(self.prints_schema).json(str(PRINTS_FILE))
        prints = prints.withColumn("value_prop_id", col("event_data").getItem("value_prop"))
        prints = prints.withColumn("timestamp", to_timestamp(col("day")))
        
        # Cargar taps
        taps = self.spark.read.schema(self.taps_schema).json(str(TAPS_FILE))
        taps = taps.withColumn("value_prop_id", col("event_data").getItem("value_prop"))
        taps = taps.withColumn("timestamp", to_timestamp(col("day")))
        
        # Cargar pays
        pays = self.spark.read.schema(self.pays_schema).csv(str(PAYS_FILE), header=True)
        pays = pays.withColumn("timestamp", to_timestamp(col("pay_date")))
        
        logger.info(f"Datos cargados: {prints.count()} prints, {taps.count()} taps, {pays.count()} pays")
        return prints, taps, pays
    
    def add_click_flag(self, prints_df, taps_df):
        """Añade la columna clicked basada en coincidencias con taps."""
        logger.info("Añadiendo flag de click...")
        
        taps_set = taps_df.select("user_id", "value_prop_id", "timestamp").dropDuplicates()
        taps_set = taps_set.withColumnRenamed("timestamp", "tap_timestamp")
        
        result = prints_df.join(
            taps_set,
            (prints_df.user_id == taps_set.user_id) &
            (prints_df.value_prop_id == taps_set.value_prop_id) &
            (prints_df.timestamp == taps_set.tap_timestamp),
            how="left"
        )
        
        result = result.drop(taps_set.user_id).drop(taps_set.value_prop_id)
        result = result.withColumn("clicked", when(col("tap_timestamp").isNotNull(), 1).otherwise(0))
        
        return result
    
    def add_historical_features(self, df_main, df_source, filter_start, filter_end, 
                              user_col="user_id", prop_col="value_prop_id", 
                              agg_col=None, new_col="recent_count", agg_func="count"):
        """
        Añade features históricas usando ventanas temporales.
        
        Args:
            df_main: DataFrame principal
            df_source: DataFrame fuente
            filter_start: Inicio de la ventana temporal
            filter_end: Fin de la ventana temporal
            user_col: Columna de usuario
            prop_col: Columna de value prop
            agg_col: Columna para agregación (para sum)
            new_col: Nombre de la nueva columna
            agg_func: Función de agregación (count o sum)
        """
        source_filtered = df_source.filter(
            (col("timestamp") >= filter_start) & (col("timestamp") < filter_end)
        )
        
        if agg_func == "count":
            aggregated = source_filtered.groupBy(user_col, prop_col).agg(
                count("*").alias(new_col)
            )
        elif agg_func == "sum" and agg_col:
            aggregated = source_filtered.groupBy(user_col, prop_col).agg(
                spark_sum(col(agg_col)).alias(new_col)
            )
        else:
            raise ValueError("Invalid agg_func or missing agg_col for sum.")
        
        result = df_main.join(aggregated, on=[user_col, prop_col], how="left")
        return result.fillna({new_col: 0})
    
    def build_dataset(self, end_date=None):
        """Construye el dataset final con todas las features."""
        logger.info("Construyendo dataset con Spark...")
        
        # Cargar datos
        prints, taps, pays = self.load_data()
        
        # Determinar fecha final
        if end_date is None:
            end_date = prints.agg({"timestamp": "max"}).collect()[0][0]
        
        # Definir ventanas temporales
        start_last_week = end_date - timedelta(days=RECENT_DAYS)
        start_3weeks_ago = end_date - timedelta(days=WINDOW_DAYS)
        
        logger.info(f"Ventana de análisis: {start_last_week} a {end_date}")
        logger.info(f"Ventana histórica: {start_3weeks_ago} a {start_last_week}")
        
        # Filtrar prints recientes
        recent_prints = prints.filter(
            (col("timestamp") >= start_last_week) & (col("timestamp") <= end_date)
        )
        
        # Añadir flag de click
        recent_prints = self.add_click_flag(recent_prints, taps)
        
        # Añadir features históricas
        recent_prints = self.add_historical_features(
            recent_prints, prints, start_3weeks_ago, start_last_week,
            new_col="print_count_3w", agg_func="count"
        )
        
        recent_prints = self.add_historical_features(
            recent_prints, taps, start_3weeks_ago, start_last_week,
            new_col="tap_count_3w", agg_func="count"
        )
        
        recent_prints = self.add_historical_features(
            recent_prints, pays, start_3weeks_ago, start_last_week,
            new_col="pay_count_3w", agg_func="count"
        )
        
        recent_prints = self.add_historical_features(
            recent_prints, pays, start_3weeks_ago, start_last_week,
            agg_col="amount", new_col="total_amount_3w", agg_func="sum"
        )
        
        logger.info("Dataset construido exitosamente con Spark")
        return recent_prints
    
    def save_dataset(self, df, filename="dataset_final_spark.csv"):
        """Guarda el dataset final."""
        from .config import OUTPUT_DIR
        
        output_path = OUTPUT_DIR / filename
        df.select(FINAL_COLUMNS).toPandas().to_csv(output_path, index=False)
        logger.info(f"Dataset guardado en: {output_path}")
    
    def run_pipeline(self, output_filename="dataset_final_spark.csv"):
        """Ejecuta el pipeline completo de Spark."""
        logger.info("Ejecutando pipeline de Spark...")
        
        try:
            dataset = self.build_dataset()
            self.save_dataset(dataset, output_filename)
            logger.info("Pipeline de Spark completado exitosamente")
            return dataset
        except Exception as e:
            logger.error(f"Error en pipeline de Spark: {str(e)}")
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Limpia recursos de Spark."""
        if self.spark:
            self.spark.stop()
            logger.info("Sesión de Spark cerrada") 