from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.sql.functions import count, avg, min, max


@dp.table(
    name="workspace.bronze.scada_lakeflow_bronze",
    comment="Raw SCADA sensor data ingested incrementally using Auto Loader"
)
def scada_lakeflow_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ";")
        .load(
            "/Volumes/workspace/bronze/scada_raw_files/"
            "autoloader_demo/incoming/"
        )
    )

@dp.table(
    name="workspace.silver.scada_lakeflow_silver",
    comment="Cleaned and typed SCADA sensor readings"
)
@dp.expect_or_drop(
    "valid_sensor_record",
    "id IS NOT NULL AND value IS NOT NULL AND unit IS NOT NULL AND timestamp IS NOT NULL"
)
def scada_lakeflow_silver():

    bronze_df = spark.readStream.table(
        "workspace.bronze.scada_lakeflow_bronze"
    )

    return (
        bronze_df
        .dropna(
            how="all",
            subset=["id", "value", "unit", "timestamp"]
        )
        .withColumn(
            "id",
            col("id").cast("int")
        )
        .withColumn(
            "value",
            when(
                col("value").rlike(r"^[0-9]+\.[0-9]+\.[0-9]+$"),
                regexp_replace(
                    col("value"),
                    r"^([0-9]+)\.([0-9]+)\.([0-9]+)$",
                    "$1$2.$3"
                )
            )
            .otherwise(col("value"))
            .cast("double")
        )
        .withColumn(
            "timestamp",
            col("timestamp").cast("timestamp")
        )
        .select(
            "id",
            "value",
            "unit",
            "timestamp"
        )
    )

@dp.materialized_view(
    name="workspace.gold.scada_lakeflow_gold",
    comment="Sensor-level SCADA analytics generated from cleaned Silver data"
)
def scada_lakeflow_gold():

    silver_df = spark.read.table(
        "workspace.silver.scada_lakeflow_silver"
    )

    return (
        silver_df
        .groupBy("id", "unit")
        .agg(
            count("*").alias("reading_count"),
            avg("value").alias("avg_value"),
            min("value").alias("min_value"),
            max("value").alias("max_value")
        )
    )

@dp.table(
    name="workspace.silver.scada_lakeflow_quarantine",
    comment="Invalid SCADA sensor records retained for investigation"
)
def scada_lakeflow_quarantine():

    bronze_df = spark.readStream.table(
        "workspace.bronze.scada_lakeflow_bronze"
    )

    return bronze_df.filter(
        col("id").isNull()
        | col("value").isNull()
        | col("unit").isNull()
        | col("timestamp").isNull()
    )