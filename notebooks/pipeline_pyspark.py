from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, count, sum as spark_sum, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, MapType, DateType
from datetime import timedelta, datetime

spark = SparkSession.builder.appName("ValuePropsPipeline").getOrCreate()

DATA_PATH = "data/"
PRINTS_FILE = DATA_PATH + "prints.json"
TAPS_FILE = DATA_PATH + "taps.json"
PAYS_FILE = DATA_PATH + "pays.csv"

prints_schema = StructType([
    StructField("day", DateType()),
    StructField("event_data", MapType(StringType(), StringType())),
    StructField("user_id", StringType()),
])
taps_schema = prints_schema
pays_schema = StructType([
    StructField("pay_date", DateType()),
    StructField("amount", DoubleType()),
    StructField("user_id", StringType()),
    StructField("value_prop_id", StringType()),
])

prints = spark.read.schema(prints_schema).json(PRINTS_FILE)
taps = spark.read.schema(taps_schema).json(TAPS_FILE)
pays = spark.read.schema(pays_schema).csv(PAYS_FILE, header=True)

prints = prints.withColumn("value_prop_id", col("event_data").getItem("value_prop"))
prints = prints.withColumn("timestamp", to_timestamp(col("day")))
taps = taps.withColumn("value_prop_id", col("event_data").getItem("value_prop"))
taps = taps.withColumn("timestamp", to_timestamp(col("day")))
pays = pays.withColumn("timestamp", to_timestamp(col("pay_date")))

end_date = prints.agg({"timestamp": "max"}).collect()[0][0]
start_last_week = end_date - timedelta(weeks=1)
start_3weeks_ago = end_date - timedelta(weeks=3)

recent_prints = prints.filter((col("timestamp") >= start_last_week) & (col("timestamp") <= end_date))

taps_set = taps.select("user_id", "value_prop_id", "timestamp").dropDuplicates()
taps_set = taps_set.withColumnRenamed("timestamp", "tap_timestamp")
recent_prints = recent_prints.join(
    taps_set,
    (recent_prints.user_id == taps_set.user_id) &
    (recent_prints.value_prop_id == taps_set.value_prop_id) &
    (recent_prints.timestamp == taps_set.tap_timestamp),
    how="left"
)
recent_prints = recent_prints.drop(taps_set.user_id).drop(taps_set.value_prop_id)
recent_prints = recent_prints.withColumn("clicked", when(col("tap_timestamp").isNotNull(), 1).otherwise(0))

from pyspark.sql import functions as F
from pyspark.sql.functions import col

def add_recent_counts(df_main, df_source, filter_start, filter_end, 
                      user_col="user_id", prop_col="value_prop_id", 
                      agg_col=None, new_col="recent_count", agg_func="count"):

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

recent_prints = add_recent_counts(
    df_main=recent_prints,
    df_source=prints,
    filter_start=start_3weeks_ago,
    filter_end=start_last_week,
    user_col="user_id",
    prop_col="value_prop_id",
    new_col="print_count_3w",
    agg_func="count"
)

recent_prints = add_recent_counts(
    df_main=recent_prints,
    df_source=taps,
    filter_start=start_3weeks_ago,
    filter_end=start_last_week,
    user_col="user_id",
    prop_col="value_prop_id",
    new_col="tap_count_3w",
    agg_func="count"
)

recent_prints = add_recent_counts(
    df_main=recent_prints,
    df_source=pays,
    filter_start=start_3weeks_ago,
    filter_end=start_last_week,
    user_col="user_id",
    prop_col="value_prop_id",
    new_col="pay_count_3w",
    agg_func="count"
)

recent_prints = add_recent_counts(
    df_main=recent_prints,
    df_source=pays,
    filter_start=start_3weeks_ago,
    filter_end=start_last_week,
    user_col="user_id",
    prop_col="value_prop_id",
    agg_col="amount",
    new_col="total_amount_3w",
    agg_func="sum"
)

final_cols = [
    "user_id", "value_prop_id", "timestamp", "clicked",
    "print_count_3w", "tap_count_3w", "pay_count_3w", "total_amount_3w"
]
recent_prints.select(final_cols).toPandas().to_csv("dataset_final.csv", index=False)

print("Dataset generado exitosamente con PySpark.")
spark.stop()