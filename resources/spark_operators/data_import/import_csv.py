import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: import_csv.py input output delimiter
    """
    spark = SparkSession\
        .builder\
        .appName("CSV Import")\
        .getOrCreate()
    
    ret_code = 3
    try:
        data = None
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        delimiter = sys.argv[3]

        data = spark.read.option('delimiter', delimiter).csv(input_path, header=True)
        data.write.format('parquet').save(output_path)
    except Exception as e:
        print(e)
        ret_code = 4

    spark.stop()
    sys.exit(ret_code) 
