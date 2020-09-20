import pymongo

from pymongo import MongoClient

class GetClient:

    def __init__(self):
        self.client=None

    def __enter__(self):
        self.client = MongoClient(username="root", password="example")
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
    val = list(pp)[0].get('maxIndex') 
    return val + 1 if val else 0
        
def create_index():
    with GetClient() as cli:
        cli.create_index([('carrier_id', pymongo.DESCENDING)], unique=True)
        cli.create_index([('index', pymongo.DESCENDING)], unique=True)
