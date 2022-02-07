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

def group_by(data_size, spark, it):

    for _ in range(it):
        gc.collect()
        t_start = timeit.default_timer()

        df = spark.read.csv(f'./data/data_{data_size}.csv', header=True)
        df.createOrReplaceTempView("df")

        ans = spark.sql(
        """
        select 
            count(*) 
        from 
            df 
        group by 
            Merk
        """
        )

        (
            ans.write
            .option("header", True)
            .mode('overwrite')
            .csv(f"./spark_output/group_by/{data_size}")
        )

        t = timeit.default_timer() - t_start
        m = memory_usage()
        write_log('group_by', 'spark', data_size, t, m)
        spark.catalog.uncacheTable("df")
        del df
        del ans
