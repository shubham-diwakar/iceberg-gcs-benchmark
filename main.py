import sys
import argparse
from operator import add
from random import random
import time
from pyspark.sql import SparkSession
from datetime import datetime
import os

# Define the path to the GCS connector JAR
gcs_connector_path = "/home/shubhamdiwakar_google_com/gcs-connector-3.0.5-shaded.jar"
# Set the credentials file location (see next section)
google_credentials = "/home/shubhamdiwakar_google_com/key.json"

def create_query_map():
    map = {}
    benchmark_types = os.listdir("queries")
    for benchmark in benchmark_types:
        map[benchmark] = os.listdir("queries/" + benchmark)
        map[benchmark].sort()
    return map

def create_spark_session(args):
    spark_builder = SparkSession.builder.appName("Query Engine on Iceberg Tables")
    spark_builder = spark_builder.master("local[*]") # Use local mode for testing; change as needed for cluster mode
    spark_builder = spark_builder.config("spark.jars", gcs_connector_path) # Path to GCS connector JAR
    spark_builder = spark_builder.config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark_builder = spark_builder.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_credentials) # Path to Google credentials JSON file
    spark_builder = spark_builder.config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2')
    spark_builder = spark_builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    spark_builder = spark_builder.config(f"spark.sql.catalog.{args.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    spark_builder = spark_builder.config(f"spark.sql.catalog.{args.catalog_name}.type", args.catalog_type)
    spark_builder = spark_builder.config(f"spark.sql.catalog.{args.catalog_name}.warehouse", args.data_warehouse_location)
    spark = spark_builder.getOrCreate()
    return spark

def calculate_query_execution_time(num_itterations, query_sql, query_num, spark):
    total_execution_times = []
    for i in range(num_itterations):
        start_time = time.time()
        try:
            result_df = spark.sql(query_sql)
            # To ensure the query executes, perform an action.
            # For benchmarking, writing to noop is common.
            result_df.write.format("noop").mode("overwrite").save()
            #result_df.show(truncate=False) # Show some results

        except Exception as e:
            print(f"Error during query: {query_num}, execution: {e}")
            # Optionally, break or continue to next run
            return -1

        end_time = time.time()
        execution_time = end_time - start_time
        total_execution_times.append(execution_time)

    if total_execution_times:
        avg_time = sum(total_execution_times) / len(total_execution_times)
    return avg_time

def store_results_to_gcs(spark, results_df, output_path):
    results_df = spark.createDataFrame(results_list)
    try:
        results_df.coalesce(1).write.mode("append").option("header", True).csv(output_path)
    except Exception as e:
        print(f"!!! ERROR: Failed to write results to GCS: {e}")

if __name__ == "__main__":
    """
    Usage: pi [partitions]
    """
    parser = argparse.ArgumentParser(description="Run queries using Spark against Iceberg.")
    parser.add_argument("--query_number", type=int, nargs= '+',default=[6], help="TPC-H query number to run (e.g., 1, 6).")
    parser.add_argument("--num_itterations", type=int, default=1, help="Number of times to run the query.")
    parser.add_argument("--data_warehouse_location", type=str, default="gs://ajayky-spark/hadoop-warehouse", help="Base path to Iceberg Warehouse (e.g., 'gs://my_bucket/warehouse/')")
    parser.add_argument("--database", type=str, default="tpch_sf1", help="Data base for TPC-H data (e.g., 1 for 1GB, 10000 for 10TB).")
    parser.add_argument("--query_type", type=str, default="tpch", help="Type of query to run can be tpch, tpcds, random (default: 'tpch').")
    parser.add_argument("--catalog_type", type=str, default="hadoop", help="Catalog type for Iceberg tables (default: 'hadoop').")
    parser.add_argument("--catalog_name", type=str, default="iceberg", help="Catalog name for Iceberg tables (default: 'iceberg').")
    parser.add_argument("--output_path", type=str, default="gs://spark-benchmark1/results", help="GCS path to write benchmark results (default: 'gs://spark-benchmark1/results').")

    query_map = create_query_map()
    args = parser.parse_args()
    spark = create_spark_session(args)
    results_list = []
    try:
        for query_num in args.query_number:
            query = (open("queries/%s/%s"%(args.query_type,query_map[args.query_type][query_num - 1]))
                        .read()
                        .replace("${database}", args.catalog_name)
                        .replace("${schema}", args.database))
            time = calculate_query_execution_time(args.num_itterations, query, query_num, spark)
            benchmark_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            results_list.append({
                "benchmark_timestamp_utc": benchmark_timestamp,
                "query_type": args.query_type,
                "query_number": query_num,
                "average_execution_time_sec": round(time, 4),
                "iterations": args.num_itterations,
                "scale_factor": args.database # Assuming this holds the scale factor like 'tpch_sf1'
            })
        # Store results to GCS
        store_results_to_gcs(spark, results_list, args.output_path)
    except Exception as e:
        print(f"An error occurred during Spark execution: {e}")
        raise
    finally:
        print("\nStopping the Spark session.")
        spark.stop()
    
    print(results_list)
