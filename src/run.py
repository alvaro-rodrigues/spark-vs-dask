import os
from sys import argv

from run_dask import run_dask, run_dask_kmenas
from run_spark import run_spark, run_spark_kmeans


def run(framework, task, data_size, file_type):
    if framework == "spark":
        run_spark(data_size, task, file_type)
    elif framework == "dask":
        run_dask(data_size, task, file_type)
    else:
        raise ValueError(f"Framework {framework} not supported")

def run_kmeans(data_size):
    run_dask_kmenas(data_size)
    run_spark_kmeans(data_size)

if __name__ == '__main__':
    framework = str(argv[1])
    task = str(argv[2])
    data_size = str(argv[3])
    file_type = str(argv[4])
    run(framework, task, data_size, file_type)