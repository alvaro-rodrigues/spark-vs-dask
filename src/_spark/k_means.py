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

def k_means(data_size, file_type):

    gc.collect()

    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .config('spark.driver.memory', '14g')
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true')
        .getOrCreate()
    )
    
    t_start = timeit.default_timer()

    if file_type == "csv":
        df = spark.read.csv(f'./data/data_{data_size}.csv', header=True)
    elif file_type == "parquet":
        df = spark.read.parquet(f'./data/parquet/data_{data_size}.parquet')
    else:
        raise ValueError(f"File type {file_type} not supported")
        
    df = (
    df.select(
        'Massa ledig voertuig',
        'Toegestane maximum massa voertuig',
        'Massa rijklaar'
    )
    )
    df = (
        df.select(
            *(F.col(c)
            .cast(DoubleType())
            .alias(c) for c in df.columns
            )
        )
    )
    df = df.na.drop()

    assemble = VectorAssembler(inputCols=[
    'Massa ledig voertuig',
    'Toegestane maximum massa voertuig',
    'Massa rijklaar'
    ], outputCol='features')

    assembled_data = assemble.transform(df)

    KMeans_algo = KMeans(
        featuresCol='features',
        k=3,
        initMode='k-means||',
        tol=0.0001,
        initSteps=2,
        maxIter=50,
        seed=42
    )
    KMeans_fit = KMeans_algo.fit(assembled_data)
    ans = KMeans_fit.transform(assembled_data)
    ans = ans.drop(F.col('features'))

    if file_type == "csv":
        (
            ans.write
            .option("header", True)
            .mode('overwrite')
            .csv(f"./spark_output/{file_type}/k_means/{data_size}")
        )
    elif file_type == "parquet":
        (
            ans.write
            .mode('overwrite')
            .parquet(f"./spark_output/{file_type}/k_means/{data_size}")
        )
    else:
        raise ValueError(f"File type {file_type} not supported")


    t = timeit.default_timer() - t_start
    m = memory_usage()
    write_log('k_means', 'spark', data_size, file_type, t, m)
    spark.stop()
    del df
    del assemble
    del assembled_data
    del KMeans_algo
    del KMeans_fit
    del ans
    