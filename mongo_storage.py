import pymongo

from pymongo import MongoClient
import settings

class GetClient:

    def __init__(self):
        self.client=None

    def __enter__(self):
        self.client = MongoClient(host=settings.MONGOURI, username=settings.MONGOUSER, password=settings.MONGOPWD)
        return self.client.cen_store.records
    
    def __exit__(self, *args, **kwargs):
        return self.client.close()

def getNextIndex():
    with GetClient() as cli:
        pp = cli.aggregate(
            [
                    {
                            "$group": 
                {
                            "_id": {},
                                "maxIndex": {"$max": "$index"}
                }
                    }
                ]
            )
    index = list(pp)
    if not index:
        return 0
    return index[0].get('maxIndex') + 1
        
def create_index():
    with GetClient() as cli:
        cli.create_index([('carrier_id', pymongo.DESCENDING)], unique=True)
        cli.create_index([('index', pymongo.DESCENDING)], unique=True)


def get_failed():
    with GetClient() as cli:
        failed = cli.find({'loaded': False})
    return len(list(failed))

create_index()