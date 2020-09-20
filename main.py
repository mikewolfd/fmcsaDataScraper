from generate_data import scrape_site, fix_store
from sql_store import dbBuild, Carrier
from mongo_storage import getNextIndex, GetClient, get_failed
import settings


def scrape(quantity=None, *args, **kwargs):
    pre = getNextIndex()
    scrape_site(settings.LOADING_FILE, quantity, *args, **kwargs)
    added = getNextIndex()
    return "{} total pages added, {} failed pages".format(added - pre, get_failed)

def build_sql(*args, **kwargs):
    pre = Carrier.select().count()
    with GetClient() as cli:
        dbBuild.populate_store(cli.find(), *args, **kwargs)
        return "{} objects generated".format(Carrier.select().count() - pre)
