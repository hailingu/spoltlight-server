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
    
    ret_code = 3
    try:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        columns = sys.argv[3].split(' ')
        data = spark.read.parquet(input_path)

        data.dropDuplicates(columns).repartition(5).write.format('parquet').save(output_path)
    except Exception as e:
        print(e)
        ret_code = 4

    spark.stop()
    sys.exit(ret_code) 