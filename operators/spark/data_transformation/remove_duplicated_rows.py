import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: remove_duplicated_rows.py input output columns 
    """
    spark = SparkSession\
        .builder\
        .appName("Remove Duplicated Rows")\
        .getOrCreate()
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    columns = sys.argv[3].split(' ')
    data = spark.read.parquet(input_path)

    data.dropDuplicates(columns).write.format('parquet').save(output_path)
    spark.stop()