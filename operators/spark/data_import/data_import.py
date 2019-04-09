import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: data import input format header output
    """
    spark = SparkSession\
        .builder\
        .appName("Data Import")\
        .getOrCreate()

    data = None
    input_path = sys.argv[1]
    file_format = sys.argv[2]
    header = True if sys.argv[3] == 'True' else False
    output_path = sys.argv[4]

    if file_format == 'csv':
        if header:
            data = spark.read.csv(input_path)
        else:
            data = spark.read.csv(input_path)

    data.write.format('parquet').save(output_path)
    spark.stop()