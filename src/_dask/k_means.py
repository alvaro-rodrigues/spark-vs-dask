import timeit
import psutil
import gc
import os
from time import sleep

import dask
import dask.dataframe as dd
from dask.distributed import Client, progress, LocalCluster

import dask_ml.cluster

exec(open("./utils/utils.py").read())

def k_means(data_size, file_type):

    gc.collect()
    if file_type == "csv":
        process = False
    elif file_type == "parquet":
        process = True
    else:
        raise ValueError(f"File type {file_type} not supported")
    with LocalCluster(processes=process, memory_limit='14GB') as cluster:
        with Client(cluster) as client:
            t_start = timeit.default_timer()

            dtype={'Aantal wielen': 'float64',
                'Europese uitvoeringcategorie toevoeging': 'object',
                'Europese voertuigcategorie toevoeging': 'object',
                'Massa ledig voertuig': 'float64',
                'Toegestane maximum massa voertuig': 'float64',
                'Massa rijklaar': 'float64',
                'Subcategorie Nederland': 'object',
                'Type gasinstallatie': 'object',
                'Rupsonderstelconfiguratiecode': 'object',
                'Datum tenaamstelling': 'float64',
                'Datum eerste toelating': 'float64',
                'Type': 'object',
                'Variant': 'object',
                'Uitvoering': 'object',
                'Code toelichting tellerstandoordeel': 'object',
                'Vervaldatum APK DT': 'object',
                'Vervaldatum tachograaf DT': 'object',
                'Zuinigheidsclassificatie': 'object',
                'Typegoedkeuringsnummer': 'object',
                'Wielbasis': 'float64'}


            if file_type == "csv":
                df = dd.read_csv(f'./data/data_{data_size}.csv', dtype=dtype)
            elif file_type == "parquet":
                df = dd.read_parquet(f'./data/parquet/data_{data_size}.parquet', engine="fastparquet")
            else:
                raise ValueError(f"File type {file_type} not supported")
            df = df[[
                'Massa ledig voertuig',
                'Toegestane maximum massa voertuig',
                'Massa rijklaar'
            ]]
            df = df.dropna()

            X = df.to_dask_array(lengths=True)
            km = dask_ml.cluster.KMeans(
                n_clusters=3,
                init='k-means||',
                tol=0.0001,
                init_max_iter=2,
                oversampling_factor=1,
                max_iter=50,
                random_state=42
            )

            km.fit(X)
            df['labels'] = dask.dataframe.from_dask_array(km.labels_, index=df.index)
            if file_type == "csv":
                df.to_csv(f"./dask_output/{file_type}/k_means/{data_size}", index=False)
            elif file_type == "parquet":
                df.to_parquet(f"./dask_output/{file_type}/k_means/{data_size}", engine="fastparquet", overwrite=True)
            else:
                raise ValueError(f"File type {file_type} not supported")

            t = timeit.default_timer() - t_start
            m = memory_usage()
            write_log('k_means', 'dask', data_size, file_type, t, m)

            del df
            del X
            del km
    