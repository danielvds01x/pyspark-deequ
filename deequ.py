"""Data Profiling

This script runs the routine of applying data profiling metrics
using the pydeequ library.
github: (https://github.com/awslabs/python-deequ)

This function receives configuration parameters,
process the analyses and saves the results in a BigQuery table.

An way to call this module would be:

gcloud dataproc jobs submit pyspark   # submit job to dataproc
    data_quality_pyspark.py           # this module
    --project my-gcp-project          # gcp project id
    --cluster my-spark-cluster        # cluster
    --region us-central1              # region
    -- gcs                            # source type (bq | gcs)
    gs://project-bucket/path/to/file.csv | .parquet
    DATA_PROFILING.ANALYSIS_RESULTS # Bigquery table where will be saved the results
    "col1 = 'a' or|and col2 = 'b'" # bq table filter optional, like a sql where clause
"""

import datetime
import logging
import os
import sys

import pandas as pd
from pydeequ import analyzers as A
from pydeequ import profiles as P
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv):
    """Run Data Profiling on table or file using Deequ library
    and save results to a bigquery table.

    Parameters
    ----------
    source_type : str
        The type of location of the table to be analyzed ('bq' or 'gcs')
    source_path : str
        The path to a gcs (cloud storage eg.: gs://path/to/file.parquet|csv) file
        or a bq (bigquery eg.: bigquery-public-data:samples.shakespeare) table.
    destination : str
        The bigquery table where the result will be saved
    staging_bucket : str
        The GCS bucket used by BigQuery connector for staging files
    app_name : str
        Spark app name
    table_filter : str, optional
        The filter applied on bq table (eg.: 'col1 = 'a' and|or col2 = 'b''). (deafult is
        '' that means no filter)

    Returns
    -------
        There's no return for this function
    """

    """Configurations"""
    # Configuration sys.argv parameters
    logging.info("Configuration sys.argv parameters")
    source_type = argv[0]  # 'bq' or 'gcs'
    source_path = argv[1]  # bq table ('dataset.table') or gcs file (gs://path/to/file.parquet)
    destination = argv[2]  # table in bq
    staging_bucket = argv[3]
    app_name = argv[4]
    table_filter = ""
    if len(argv) > 5:
        table_filter = argv[5]  # [optional] filter table, e.g.: 'col1 = a and|or col2 = b'

    # Config Spark Session
    logging.info("Config Spark Session")
    spark = SparkSession.builder.master("yarn").appName(app_name).getOrCreate()

    # Config Temp GCS Bucket
    logging.info("Config Temp GCS Bucket")
    spark.conf.set("temporaryGcsBucket", staging_bucket)

    """Read GCS File or Bigquery Table"""
    # Getting file extension
    file_extension = os.path.splitext(source_path)[1]

    # Read File or Table
    logging.info("Read File or Table")
    df = None
    if source_type == "gcs":
        logging.info(f"Reading from gcs file: {source_path}")
        if file_extension == ".parquet":
            df = spark.read.load(source_path)

        elif file_extension == ".csv":
            df = spark.read.options(
                inferSchema="True", header=True, delimiter="\t"
            ).csv(source_path)
    elif source_type == "bq":
        logging.info(f"Reading from bq table: {source_path}")
        if table_filter == "":
            df = (
                # Load data from BigQuery.
                spark.read.format("bigquery")
                .option('table', source_path)
                .load()
                .select("*")
            )
        else:
            df = (
                # Load data from BigQuery.
                spark.read.format("bigquery")
                .option('table', source_path)
                .load()
                .select("*")
                .filter(table_filter)
            )
    else:
        logging.info(f"Unknown value {argv[0]} for source_type parameter")
        sys.exit(1)

    df = df.drop("METADATA")

    # Executing Profile to get Datatype for each column
    logging.info("Executing Profile to get Datatype for each column")
    result = P.ColumnProfilerRunner(spark).onData(df).run()

    # Getting columns names and types
    d = []
    for col, profile in result.profiles.items():
        d.append({"instance": col, "datatype": profile.dataType})

    df_column_types = spark.createDataFrame(pd.DataFrame(d))

    """Build and Run Profiles and Analyzers"""
    logging.info("Build and Run Profiles and Analyzers")
    # Setting Analysis
    analysis_result = A.AnalysisRunner(spark).onData(df)

    for col, profile in result.profiles.items():
        if profile.dataType in ("String", "Unknown"):
            analysis_result = (
                analysis_result.addAnalyzer(A.Completeness(col))
                .addAnalyzer(A.CountDistinct(col))
                .addAnalyzer(A.Distinctness(col))
                # .addAnalyzer(A.Size())
                # .addAnalyzer(A.UniqueValueRatio(col))
                # .addAnalyzer(A.Uniqueness(col))
                # Error: https://github.com/awslabs/python-deequ/issues/8
            )
            if profile.dataType in "String":
                analysis_result = analysis_result.addAnalyzer(
                    A.MinLength(col)
                ).addAnalyzer(A.MaxLength(col))

        elif profile.dataType in ("Integral", "Fractional"):
            analysis_result = (
                analysis_result.addAnalyzer(A.Maximum(col))
                .addAnalyzer(A.Mean(col))
                .addAnalyzer(A.Minimum(col))
                .addAnalyzer(
                    A.ApproxQuantiles(col, quantiles=[0.05, 0.25, 0.5, 0.75, 0.95])
                )
                .addAnalyzer(A.StandardDeviation(col))
                .addAnalyzer(A.Completeness(col))
                .addAnalyzer(A.Distinctness(col))
            )

    # Run analysis and save results
    analysis_result_df = A.AnalyzerContext.successMetricsAsDataFrame(
        spark, analysis_result.run()
    )

    # Adding the other fields
    logging.info("Adding the other fields on final result dataframe")
    analysis_result_df = (
        analysis_result_df.join(df_column_types, on=["instance"], how="inner")
        .withColumn("source_type", F.lit(source_type))
        .withColumn("source_path", F.lit(source_path))
        .withColumn("filter", F.lit(table_filter))
        .withColumn("created", F.current_timestamp())
        .withColumn("updated", F.lit(datetime.datetime.now()))
    )

    """Save results"""
    # Load results to bigquery table
    logging.info(f"Load results to bigquery destination table: {destination}")
    analysis_result_df = analysis_result_df.withColumn("source_path", F.lit(source_path))
    analysis_result_df.write.format("bigquery").mode("append").save(destination)


if __name__ == "__main__":
    main(sys.argv[1:])
