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

def get_and_store(items, index):
    o = []
    for _index, value in enumerate(items):
        val = scrape.get_carrier_registration(value)
        val['index'] = _index + index
        o.append(val)
    with mongo_storage.GetClient() as cli:
        cli.insert_many(o)


def run_tasks(values, quantity=5000, max_workers=16):
    index = mongo_storage.getNextIndex()
    task_runner(get_and_store, values, index, quantity, max_workers)


def scrape_site(filepath, quantity=None, *args, **kwargs):
    census_data = pd.read_csv(filepath, encoding="ISO-8859-1")
    dot_numbers = census_data.DOT_NUMBER.to_list()
    run_tasks(dot_numbers, quantity, *args, **kwargs)
