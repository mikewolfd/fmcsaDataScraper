import concurrent.futures
import mongo_storage
import pandas as pd
import scrape
import time
from peewee import chunked
import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.ERROR)

def task_runner(func, values, index=0, quantity=None, max_workers=16, chunk_size=100, *args, **kwargs):
    """ 
    This slices an array as needed, chunks it up, and launches tasks
    """
    quant = index + quantity if quantity else None
    _index = index
    vals = values[index:quant]
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = set()
        for items in chunked(vals, chunk_size):
            kwargs['index'] = _index
            futures.add(executor.submit(func, items, *args, **kwargs))
            _index += len(items)
        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except Exception as exec :
                raise
            index += len(items)

def get_and_store(items, index, *args, **kwargs):
    """ 
    This is the primary task, it creates temp objects in mongo to maintain an index, gets the scraped data and inserts it into mongo
    """
    o = []
    false_items = []
    query = kwargs.get('query', {'index': {'$gte': index, '$lt': index + len(items)}})
    for _index, value in enumerate(items):
        false_items.append({'carrier_id': value, 'loaded': False, 'index': _index + index})
    with mongo_storage.GetClient() as cli:
        cli.insert_many(false_items)
        for _index, value in enumerate(items):
            val = scrape.get_carrier_registration(value)
            val['index'] = _index + index
            o.append(mongo_storage.pymongo.ReplaceOne({'carrier_id': value}, val))
        cli.bulk_write(o, ordered=False)


def run_tasks(values, quantity=5000, max_workers=16, *args, **kwargs):
    """ 
    This sets more reasonable debug variables, gets the most recent index
    """
    index = mongo_storage.getNextIndex()
    task_runner(get_and_store, values, index, quantity, max_workers)


def scrape_site(filepath, quantity=None, max_tries=5, cooldown=5, *args, **kwargs):
    """ 
    This imports the csv, pulls the column data and launches the tasks
    max_tries is for retry incase of connection or db access failure 
    cooldown is in minutes
    """
    census_data = pd.read_csv(filepath, encoding="ISO-8859-1")
    dot_numbers = census_data.DOT_NUMBER.to_list()
    init = True
    for i in range(max_tries):
        try:
            if not init:
                time.sleep(cooldown*60)
            run_tasks(dot_numbers, quantity, *args, **kwargs)
            break
        except Exception as exec:
            logging.error('{}'.format(exec)) 
            init = False
            continue

def fill_failed(values, *args, **kwargs):
    """ 
    This is the repair task, it retrys the scrape and updates exisiting 
    """
    o =[]
    with mongo_storage.GetClient() as cli:
        for value in values:
            val = scrape.get_carrier_registration(value)
            val['loaded'] = True
            o.append(mongo_storage.pymongo.UpdateOne({'carrier_id': value}, {"$set": val }))
        cli.bulk_write(o, ordered=False)

def fix_store():
    """ 
    This is the repair function, it finds all failed tasks and retrys loading the data
    """
    with mongo_storage.GetClient() as cli:
        failed = cli.aggregate([{'$match': {'loaded': False}}, {'$group': { "carrier_id": {"$push": "$carrier_id"}, "_id": None}}])
    failed = list(failed)
    if failed:
        _ids = list(failed)[0].get('carrier_id')
        return task_runner(fill_failed, _ids, index=0)
    return False