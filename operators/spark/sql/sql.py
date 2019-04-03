import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: sql input output sql
    """
    spark = SparkSession\
        .builder\
        .appName("sql")\
        .getOrCreate()

    data = spark.read.parquet(sys.argv[0])
    output = sys.argv[1]
    sql = sys.argv[2]

    def prepare_sql(sql):
        return sql

    spark.sql(prepare_sql(sql)).write.format('parquet').save(output)
    spark.stop()