import timeit
import psutil
import gc
import os


from _spark import (
    load_write,
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


def run_spark(data_size, task, file_type):

    tasks = {
        "load_write": load_write,
        "filter": filter,
        "group_by": group_by,
        "join": join,
        "self_join": self_join,
        "sort": sort
    }

    task_func = tasks.get(task)
    task_func(data_size, file_type)

def run_spark_kmeans(data_size, file_type):

    k_means(data_size, file_type)
