import timeit
import psutil
import gc
import os

from _spark import (
    filter,
    group_by,
    join,
    k_means,
    self_join,
    sort
)

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext, DataFrame, functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans


def run_spark(data_size, it=1):
    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .config('spark.driver.memory', '14g')
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true')
        .getOrCreate()
    )

    filter(data_size, spark, it)
    group_by(data_size, spark, it)
    join(data_size, spark, it)
    self_join(data_size, spark, it)
    sort(data_size, spark, it)

    spark.stop()

def run_spark_kmeans(data_size, it=1):
    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .config('spark.driver.memory', '14g')
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true')
        .getOrCreate()
    )

    k_means(data_size, spark, it)

    spark.stop()