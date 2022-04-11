import timeit
import psutil
import gc
import os

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrame, functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

exec(open("./utils/utils.py").read())

def load_write(data_size, spark, it):

    for _ in range(it):
        gc.collect()
        t_start = timeit.default_timer()

        df = spark.read.csv(f'./data/data_{data_size}.csv', header=True)
        ans = df

        (
            ans.write
            .option("header", True)
            .mode('overwrite')
            .csv(f"./spark_output/load_write/{data_size}")
        )

        t = timeit.default_timer() - t_start
        m = memory_usage()
        write_log('load_write', 'spark', data_size, t, m)
        del df
        del ans
