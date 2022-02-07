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

def join(data_size, spark, it):

    for _ in range(it):
        gc.collect()
        t_start = timeit.default_timer()

        df = spark.read.csv(f'./data/data_{data_size}.csv', header=True)
        df = df.drop(F.col('type'))
        df.createOrReplaceTempView("df")
        join_df = spark.read.csv('./data/join_data.csv', header=True)
        join_df.createOrReplaceTempView("join_df")


        ans = spark.sql(
        """
        select 
            * 
        from 
            df 
        join 
            join_df 
            on df.Inrichting = join_df.type
        """
        )

        (
            ans.write
            .option("header", True)
            .mode('overwrite')
            .csv(f"./spark_output/join/{data_size}")
        )

        t = timeit.default_timer() - t_start
        m = memory_usage()
        write_log('join', 'spark', data_size, t, m)
        spark.catalog.uncacheTable("df")
        spark.catalog.uncacheTable("join_df")
        del df
        del ans
        del join_df
