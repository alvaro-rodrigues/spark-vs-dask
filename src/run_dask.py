import timeit
import psutil
import gc
import os

from _dask import (
    filter,
    group_by,
    join,
    k_means,
    self_join,
    sort
)

import dask
import dask.dataframe as dd
from dask.distributed import Client, progress
import dask_ml.cluster


def run_dask(data_size, it=1):

    client = Client(
        n_workers=4,
        processes=True,
        threads_per_worker=4,
        memory_limit='4GB'
    )

    os.environ['DASK_SCHEDULER_ADDRESS'] = 'tcp://localhost:8786'

    filter(data_size, it)
    group_by(data_size, it)
    join(data_size, it)
    self_join(data_size, it)
    sort(data_size, it)

    client.shutdown()


def run_dask_kmenas(data_size, it=1):

    client = Client(
        n_workers=4,
        processes=True,
        threads_per_worker=4,
        memory_limit='4GB'
    )

    os.environ['DASK_SCHEDULER_ADDRESS'] = 'tcp://localhost:8786'

    k_means(data_size, it)

    client.shutdown()

    