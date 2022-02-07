import timeit
import psutil
import gc
import os

import dask
import dask.dataframe as dd
from dask.distributed import Client
import dask_ml.cluster

exec(open("./utils/utils.py").read())

def sort(data_size, it=1):

    os.environ['DASK_SCHEDULER_ADDRESS'] = 'tcp://localhost:8786'

    for _ in range(it):
        gc.collect()
        t_start = timeit.default_timer()

        dtype={'Aantal wielen': 'float64',
            'Europese uitvoeringcategorie toevoeging': 'object',
            'Europese voertuigcategorie toevoeging': 'object',
            'Massa ledig voertuig': 'float64',
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


        df = dd.read_csv(f'./data/data_{data_size}.csv', dtype=dtype)

        ans = df.sort_values(by="Datum tenaamstelling")
        ans.to_csv(f"./dask_output/sorting/{data_size}", index=False)

        t = timeit.default_timer() - t_start
        m = memory_usage()
        write_log('sort', 'dask', data_size, t, m)
        del df
        del ans
        del dtype