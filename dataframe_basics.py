
'''
To Be run in ipython 
API for dask data frames - implement a smaller subset than pandas 
https://docs.dask.org/en/latest/dataframe-api.html

Dataframe IO
https://docs.dask.org/en/latest/dataframe-create.html

'''
import makedata

data = makedata.data() #get dask dataframe

data.dtypes     #inspect datatypes

data.head(5)       #inspect data

# Common Dask Dataframe operations are identical to pandas

#filter operation
data2 = data[data.age > 60]

#group by operation
data.groupby('occupation').income.mean().compute()

#sort operation
data.occupation.value_counts().nlargest(5).compute()

#write to csv
data[data.city == 'Madison Heights'].compute().to_csv('Madison.csv')


#Exercise1: What occupation is Meggan Mayo

data[data.name == 'Meggan Mayo'].occupation.compute()

#Excersie2: How many people are there in the city 'Sun Prairie' that age over the age of 35
data[(data.city == 'Sun Prairie') & (data.age > 35)].compute()

# Exercise3: Find the 10th most populous cities
data.groupby('city').name.count().nlargest(10).compute()

#Exercise4: write a function that adds a new column that gives everyone a one dollar raise 
data.assign(income_increase = data.income + 1).compute()

#Exercise5: what is the standard deviation of income grouped by age. This should reveal a data secret.
data.groupby('age').income.std().compute()








