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

def self_join(data_size, spark, it):

    for _ in range(it):
        gc.collect()
        t_start = timeit.default_timer()

        df = spark.read.csv(f'./data/data_{data_size}.csv', header=True)

        ans = (
            df.alias("df1")
            .join(df.alias("df2"),
                F.col("df1.Kenteken") == F.col("df2.Kenteken"),
                "left"
            )
        )

        cols = []
        cols += [str(f"{c}_1") for c in df.columns]
        cols += [str(f"{c}_2") for c in df.columns]

        ans = ans.toDF(*cols)

        (
            ans.write
            .option("header", True)
            .mode('overwrite')
            .csv(f"./spark_output/self_join/{data_size}")
        )

        t = timeit.default_timer() - t_start
        m = memory_usage()
        write_log('self_join', 'spark', data_size, t, m)
        del df
        del ans
        del cols
    