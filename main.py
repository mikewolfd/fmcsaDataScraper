from generate_data import scrape_site
from sql_store import dbBuild, Carrier
from mongo_storage import getNextIndex, GetClient, get_failed
import settings


def scrape(quantity=None):
    pre = getNextIndex()
    scrape_site(settings.LOADING_FILE, quantity)
    added = getNextIndex()
    return "{} total pages added, {} failed pages".format(added - pre, get_failed)

def build_sql():
    pre = Carrier.select().count()
    with GetClient() as cli:
        dbBuild.populate_store(cli.find())
        return "{} objects generated".format(Carrier.select().count() - pre)
