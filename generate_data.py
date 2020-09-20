import concurrent.futures
import mongo_storage
import pandas as pd
import scrape
from peewee import chunked
# import logging
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

def task_runner(func, values, index, quantity=None, max_workers=16, chunk_size=100, *args, **kwargs):
    quant = index + quantity if quantity else None
    vals = values[index:quant]
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for items in chunked(vals, chunk_size):
            # logging.info('test {}'.format(len(items)))
            kwargs['index'] = index
            z = executor.submit(func, items, *args, **kwargs)
            # logging.info('{}'.format(z.result()))
            index += len(items)

def get_and_store(items, index, *args, **kwargs):
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
    index = mongo_storage.getNextIndex()
    task_runner(get_and_store, values, index, quantity, max_workers)


def scrape_site(filepath, quantity=None, *args, **kwargs):
    census_data = pd.read_csv(filepath, encoding="ISO-8859-1")
    dot_numbers = census_data.DOT_NUMBER.to_list()
    run_tasks(dot_numbers, quantity, *args, **kwargs)

def fill_failed(values, *args, **kwargs):
    o =[]
    with mongo_storage.GetClient() as cli:
        for value in values:
            val = scrape.get_carrier_registration(value)
            val['loaded'] = True
            o.append(mongo_storage.pymongo.UpdateOne({'carrier_id': value}, {"$set": val }))
        cli.bulk_write(o, ordered=False)

def fix_store():
    with mongo_storage.GetClient() as cli:
        failed = cli.aggregate([{'$match': {'loaded': False}}, {'$group': { "carrier_id": {"$push": "$carrier_id"}, "_id": None}}])
    failed = list(failed)
    if failed:
        _ids = list(failed)[0].get('carrier_id')
        return task_runner(fill_failed, _ids, index=0)
    return False