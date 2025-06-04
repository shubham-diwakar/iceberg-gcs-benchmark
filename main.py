import sys
from operator import add
from random import random
from pyspark.sql import SparkSession
import os

from pyspark.sql import SparkSession


def create_query_map():
    map = {}
    benchmark_types = os.listdir("queries")
    for benchmark in benchmark_types:
        map[benchmark] = os.listdir("queries/" + benchmark)
        map[benchmark].sort()
    return map


if __name__ == "__main__":
    """
    Usage: pi [partitions]
    """
    query_map = create_query_map()
    print(query_map)
    query = (open("queries/tpcds/%s"%(query_map['tpcds'][0]))
             .read()
             .replace("${database}", "mydatabase")
             .replace("${schema}", "mychema"))
    print(query)
    # spark = SparkSession.builder.appName("IcebergBenchmark").getOrCreate()
    #
    # spark.stop()
