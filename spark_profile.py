from pyspark.sql import SparkSession
import os

def create_query_map():
    map = {}
    benchmark_types = os.listdir("iceberg-gcs-benchmark/queries")
    for benchmark in benchmark_types:
        map[benchmark] = os.listdir("iceberg-gcs-benchmark/queries/" + benchmark)
        map[benchmark].sort()
    return map

spark = SparkSession.builder \
    .appName("TPC-H Parquet to Iceberg Migration") \
    .master("local[*]") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "gs://ajayky-spark/hadoop-warehouse") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()

query_map = create_query_map()

query_type= "tpch" # Change this to the desired query type
catalog_name = "iceberg" # Change this to the desired catalog name
database = "tpch_sf1" # Change this to the desired database

for key in query_map.keys():
    if key == query_type:
        query = (open("iceberg-gcs-benchmark/queries/%s/%s"%(query_type,query_map[query_type][i]))
                            .read()
                            .replace("${database}", catalog_name)
                            .replace("${schema}", database))
        spark.sql(query).write.format("noop").mode("overwrite").save()