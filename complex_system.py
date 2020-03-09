from dask import delayed
import makedata
import dask.array as da
import numpy as np

@delayed
def increment(x):
    return x + 1


@delayed
def halve(y):
    return y / 2


@delayed
def default(hist, income):
    return hist**2 + income


@delayed
def agg(x, y):
    return x + y


def merge(seq):
    if len(seq) < 2:
        return seq
    middle = len(seq)//2
    left = merge(seq[:middle])
    right = merge(seq[middle:])
    if not right:
        return left
    return [agg(left[0], right[0])]

class Default():
    inputs = ['inc_hist', 'halved_income']
    outputs = ['defaults']

    @delayed
    def equation(self, inc_hist, halved_income, **kwargs):
        return inc_hist**2 + halved_income

@delayed
def agerisk(age):
    return (100 - age)/2 

def main():
    hist_yrs = range(10)
    incomes = range(10)
    inc_hist = [increment(n) for n in hist_yrs]
    halved_income = [halve(n) for n in incomes]
    estimated_defaults = [default(hist, income) for hist, income in zip(inc_hist, halved_income)]
    total_defaults = sum(estimated_defaults)
    print(total_defaults)
    total_defaults.compute
    print(total_defaults.compute())
    total_defaults.visualize() # requires graphviz and python-graphviz to be install
  
def using_default_class():
    """ Exercise to do: implement on larger dataset using dask rather than theoretical example
    """
    data = makedata.data()
    #data = data.compute() 
    print(data.income)
    print(data.dtypes)
    
    data["halve_income"] = data.income.map(halve.compute())
    data["inc_hist"] = data.age.map(agerisk.compute())
    data["default"] = data.apply(lambda data : default(data.inc_hist,data.halve_income).compute() , axis=1,)

    default_total = data.default.sum()
    default_total = default_total.compute()/1000000
    print("defaults of portfolio are: ",default_total, " million")
if __name__ == "__main__":
    #using_default_class()
    main()


