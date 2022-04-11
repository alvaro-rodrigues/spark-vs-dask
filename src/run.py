import os

from run_dask import run_dask, run_dask_kmenas
from run_spark import run_spark, run_spark_kmeans


def run(data_size, it=1):
    run_dask(data_size, it)
    run_spark(data_size, it)

def run_kmeans(data_size, it=1):
    run_dask_kmenas(data_size, it)
    run_spark_kmeans(data_size, it)

if __name__ == '__main__':
    run('100k', 10)
    run('1M', 10)
    run('5M', 10)
    run('10M', 10)
    run_kmeans('100k', 10)
    run_kmeans('1M', 10)
    run_kmeans('5M', 10)
    run_kmeans('10M', 10)
