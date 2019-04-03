import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: remove_duplicated_rows input output [columns...]
    """
    spark = SparkSession\
        .builder\
        .appName("Remove Duplicated Rows")\
        .getOrCreate()

    data = spark.read.parquet(sys.argv[0])
    columns = []
    output = sys.argv[1]

    i = 2
    while i < len(sys.argv):
        columns.append(sys.argv[i])

    data.dropDuplicates(columns).write.format('parquet').save(output)
    spark.stop()