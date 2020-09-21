from generate_data import scrape_site, fix_store
from sql_store import dbBuild, Carrier
from mongo_storage import getNextIndex, GetClient, get_failed
import settings


def scrape(quantity=None, max_tries=10, cooldown=5, max_workers=16, *args, **kwargs):
    """ 
    Scrapes the site data based on settings in .env, if quantity is None it will load all available files.
    max_tries is for retry incase of connection or db access failure 
    cooldown is in minutes
    max_workers is to set the maximum number of process workers accessing the site and loading data 
    """
    pre = getNextIndex()
    scrape_site(settings.LOADING_FILE, quantity, max_tries=max_tries, cooldown=cooldown, max_workers=max_workers, *args, **kwargs)
    added = getNextIndex()
    return "{} total pages added, {} failed pages".format(added - pre, get_failed)

def build_sql(*args, **kwargs):
    """ 
    Builds an sqlite3 db based on the mode structure in sql_store.
    The File is stored in the data dir specified in the .env
    """
    pre = Carrier.select().count()
    with GetClient() as cli:
        dbBuild.populate_store(cli.find(), *args, **kwargs)
        return "{} objects generated".format(Carrier.select().count() - pre)
