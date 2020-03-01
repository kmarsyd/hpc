'''
Estimation of pi using dask library 

'''


import numpy.random as rng
import time
import dask

def pi_run(num_trials):
    """Calculate pi based on points with a box and circle of radius 1
    """
    r = 1.0
    r2 = r * r
    Ncirc = 0
    for _ in range(num_trials):
        x = rng.random() 
        y = rng.random()
        if ( x**2 + y**2 < r2 ):
            Ncirc +=1 
    pi = 4.0 * ( float(Ncirc) / float(num_trials) ) 
    return pi

def cycle_pi():
    """Cycle through a list of trials of increasing magnitude
    using dask delayed to create lazy set of functions
    """
    num_trials = [10**1,10**2,10**3,10**4,10**5,10**6,10**7]
    lazy_pi_estimates = []
    
    for trial in num_trials:
        lazy_result = dask.delayed(pi_run)(trial)
        lazy_pi_estimates.append(lazy_result)

    return lazy_pi_estimates

if __name__ == "__main__":
    from dask.distributed import Client
    client = Client()
    print(client)
    start_time = time.time()
    pi = cycle_pi()

    #trigger the computation
    result = dask.compute(*pi)
    duration = time.time() - start_time
    #notice pi is now a set of lazy results
    pi[0]
    print(f"Pi convergence with dask : {result} calculated in {duration} seconds")
