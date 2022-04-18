import timeit
import psutil
import gc
import os


from _dask import (
    load_write,
    filter,
    group_by,
    join,
    k_means,
    self_join,
    sort
)

import dask
import dask.dataframe as dd
from dask.distributed import Client, progress, LocalCluster
import dask_ml.cluster


def run_dask(data_size, task, file_type):

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

def run_dask_kmenas(data_size, file_type):

    k_means(data_size)
    