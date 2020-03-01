
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import dask as dk
import time


def load_data():
 
    #load two years worth of data 
    #ds = xr.open_mfdataset(['../data/conus_daily_eighth_2008_ens_mean.nc4','../data/conus_daily_eighth_2009_ens_mean.nc4'],
    #                    engine='netcdf4', concat_dim='ensemble', chunks={'time': 366})

    #load many years with data spill over to specified location
    ds = xr.open_mfdataset('../data/conus*201*',engine='netcdf4',concat_dim='ensemble',chunks={'time':366})
    print('ds size in GB {:0.2f}\n'.format(ds.nbytes / 1e9))

    ds.info()
    return ds

def main():
    from dask.distributed import Client
    client = Client(n_workers=1,local_directory='/scratch/Training/kmar7637')
    client

    ds = load_data()
    ds = ds.fillna(value=0)
    for name, da in ds.data_vars.items():
        print(name, da.data)

    # calculates the long term mean along the time dimension
    da_mean = ds['t_mean'].mean(dim='time')

    # calculate the intra-ensemble range of long term means
    da_spread = da_mean.max(dim='ensemble') - da_mean.min(dim='ensemble')
    da_spread.plot(robust=True, figsize=(10, 6))
    plt.title('Intra-ensemble range in mean annual temperature')
    plt.savefig('variance_temp.png')
    
    #most of the computation time was spend loading data to disk. 
    #We can have this data persist in memory for quicker retreval
    t_mean = ds['t_mean'].isel(ensemble=slice(0, 25))
    t_mean = t_mean.persist()
    t_mean #this now resides in memory on our workers.

    #repeating the process using data in persistant memory
    temp_mean = t_mean.mean(dim='time')
    spread = temp_mean.max(dim='ensemble') - temp_mean.min(dim='ensemble')  # calculates the intra-ensemble range of long term means
    mean = spread.compute()
    mean.plot(robust=True, figsize=(10, 6))
    plt.title('Intra-ensemble range in mean annual temperature 2')
    plt.savefig('variance_temp2')

    print("done")

if __name__=="__main__":
    tic = time.time()
    main()
    duration = time.time() - tic
    print("runtim of main code: ", duration, " seconds")
