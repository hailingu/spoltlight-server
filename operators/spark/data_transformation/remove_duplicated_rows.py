import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: remove_duplicated_rows input columns output
    """
    spark = SparkSession\
        .builder\
        .appName("Remove Duplicated Rows")\
        .getOrCreate()

    data = spark.read.parquet(sys.argv[1])
    columns = sys.argv[2].split(' ')
    output = sys.argv[3]

    data.dropDuplicates(columns).write.format('parquet').save(output)
    spark.stop()