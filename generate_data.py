import concurrent.futures
import mongo_storage
import pandas as pd
import scrape


def chunkify(lst, chunk_size=500):
    for x in range(0, len(lst), chunk_size):
        yield lst[x:x + chunk_size]


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
    vals = values[index:index+quantity]
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for items in chunkify(vals, chunk_size=100):
            executor.submit(get_and_store, items, index)
            index += len(items)

def scrape_site(filepath, quantity=None, *args, **kwargs):
    census_data = pd.read_csv(filepath, encoding = "ISO-8859-1")
    dot_numbers = census_data.DOT_NUMBER.to_list()
    quant = quantity or len(dot_numbers) - mongo_storage.getNextIndex()
    run_tasks(dot_numbers, quant, *args, **kwargs)