

import dask
import dask.dataframe as dd
import json
import os
import numpy as np

def flatten(record):
    """ nested to flat structure of json records
    """
    return {
        'age': record['age'],
        'occupation': record['occupation'],
        'telephone': record['telephone'],
        'credit-card-number': record['credit-card']['number'],
        'credit-card-expiration': record['credit-card']['expiration-date'],
        'name': ' '.join(record['name']),
        'street-address': record['address']['address'],
        'city': record['address']['city']
    }


def make_data_people():
    """ return dask bag of json objects and write to disk
    """ 
    os.makedirs('data', exist_ok=True)              # Create data/ directory
    b = dask.datasets.make_people()                 # Make records of people
    b.map(json.dumps).to_textfiles('./data/*.json')   # Encode as JSON, write to disk
    return b
    
def estimate_income(age):
    """ generate income correlated with age
    """
    return np.power(age,2)*40


def data():
    """ returns dask dataframe from a dask bag that is augmented with extra info
    """
    data = make_data_people()
    #convert to dask dataframe
    df = data.map(flatten).to_dataframe()
    df["income"] = estimate_income(df["age"])
    return df

if __name__ == "__main__":
    df = data()

