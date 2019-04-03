import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: data split input output1 output2 percentage
    """
    spark = SparkSession\
        .builder\
        .appName("Data Split")\
        .getOrCreate()

    data = spark.read.parquet(sys.argv[0])
    output1 = sys.argv[1]
    output2 = sys.argv[2]
    percentage = float(sys.argv[3])
    split_percentage_array = []
    split_percentage_array.append(percentage)
    split_percentage_array.append(1 - percentage)
    data_splits = data.randomSplit(split_percentage_array)
    data_splits[0].write.format('parquet').save(output1)
    data_splits[1].write.format('parquet').save(output2)
    spark.stop()