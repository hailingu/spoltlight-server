import sys
from random import random
from operator import add

import numpy
from numpy import allclose

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import *
from pyspark.ml.feature import VectorAssembler,StringIndexer
from pyspark.sql.types import DoubleType


if __name__ == "__main__":
    """
        Usage: random_forest input  
    """
    spark = SparkSession\
        .builder\
        .appName("Random Forest Algorithm")\
        .getOrCreate()
    
    input_path = sys.argv[1]
    feature_cols = sys.argv[2].columns.strip("'").split(' ')
    label_col = sys.argv[3]

    
    data = spark.read.parquet(input_path)

    for feature in feature_cols:
        data = data.withColumn(feature, data[feature].cast(DoubleType()))

    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    data = assembler.transform(data)

    data = data.select(['features', label_col])
    label_indexer = StringIndexer(inputCol=label_col, outputCol='label').fit(data)
    data = label_indexer.transform(data)

    data = data.select(['features', 'label'])
    train, test = data.randomSplit([0.70, 0.30])

    rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="label", seed=42)
    model = rf.fit(train)
    prediction = model.transform(test)
    print("Prediction")
    prediction.show(10)

    print('feature importance')
    print(model.featureImportances)

    spark.stop()