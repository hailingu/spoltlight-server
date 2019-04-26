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
        .appName("sql")\
        .getOrCreate()
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    sql_str = sys.argv[3]
    data = spark.read.parquet(input_path)

    data.sql(sql_str).write.format('parquet').save(output_path)
    spark.stop()