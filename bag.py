'''
Dask Bag for data operations that use parallel and memory efficient processes.

Content slightly altered from below dask example:  
https://examples.dask.org/applications/json-data-on-the-web.html

'''
import dask.bag as db
import json
import os
import re
import time
def daskbagtutorial():
        
    #investigate underlying data by reading text file that houses daily data into a bag. First few rows are displayed
    db.read_text('https://archive.analytics.mybinder.org/events-2018-11-03.jsonl').take(3)


    #Index file loaded as a bag. No data transfer or computation kicked off - just organising mapping the file to a json structure
    index = db.read_text('https://archive.analytics.mybinder.org/index.jsonl').map(json.loads)
    print(index)

    #data transfer and computation triggered by a compute
    db.read_text('https://archive.analytics.mybinder.org/index.jsonl').map(json.loads).compute()

    #In order to perform data manipulations, commands can be chained together to generate a sequence of actions.
    # some examples are

    #find a row in the unstructured data
    one_record = (db.read_text('https://archive.analytics.mybinder.org/index.jsonl').map(json.loads).filter(lambda record: record['date'] == '2020-02-20').compute())
    one_record

    # or a series of rows

    prog = re.compile('2020-02-..')
    one_record = (db.read_text('https://archive.analytics.mybinder.org/index.jsonl').map(json.loads).filter(lambda record: prog.match(record['date'])).compute())

    #or filter rows with the assistance of a temporary typecast.

    db.read_text('https://archive.analytics.mybinder.org/index.jsonl').map(json.loads).filter(lambda record: int(record['count']) > 1000).compute()


    # Lets set up aggregating the files in one dask bag
    filenames = (db.read_text('https://archive.analytics.mybinder.org/index.jsonl')
                .map(json.loads)
                .pluck('name')
                .compute())

    filenames = ['https://archive.analytics.mybinder.org/' + fn for fn in filenames]
    filenames[:5]

    #Create a dask bag around the list of urls calling json load on every one that turns json test to python dictionaries

    events = db.read_text(filenames).map(json.loads)
    events.take(2)

    #
    events
    #you can control the partitions into which the data is binned
    events = db.read_text(filenames,files_per_partition=2).map(json.loads)
    events

    # dask can be used to perform further aggregation work. Allows unstructured data manipulation
    #however it could be useful to
    events.pluck('spec').frequencies(sort=True).take(20)

    #We can finally convert the dask bag to the familiar python dataframe for more work
    df = events.to_dataframe()
    df.head()

    df.spec.value_counts().nlargest(20).to_frame().compute()

    #assuming the dask bag fits into memory, we can store it in RAM to avoid continuously downloading
    df = df.persist()

    df

if __name__ == '__main__':
    tic = time.time()
    daskbagtutorial()
    toc = time.time()
    print("runtime: ",(toc - tic)/60," mins")