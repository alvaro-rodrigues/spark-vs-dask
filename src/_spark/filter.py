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

def filter(data_size, file_type):

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

    
    df.createOrReplaceTempView("df")

    ans = spark.sql(
    """
    select 
        * 
    from 
        df 
    where 
        Voertuigsoort like '%Personenauto%' 
        and (Merk like 'MERCEDES-BENZ' or Merk like 'BMW' ) 
        and Inrichting like '%hatchback%'
    """
    )

    if file_type == "csv":
        (
            ans.write
            .option("header", True)
            .mode('overwrite')
            .csv(f"./spark_output/{file_type}/filter/{data_size}")
        )
    elif file_type == "parquet":
        (
            ans.write
            .mode('overwrite')
            .parquet(f"./spark_output/{file_type}/filter/{data_size}")
        )
    else:
        raise ValueError(f"File type {file_type} not supported")

    t = timeit.default_timer() - t_start
    m = memory_usage()
    write_log('filter', 'spark', data_size, file_type, t, m)
    spark.catalog.uncacheTable("df")
    spark.stop()
    del df
    del ans
