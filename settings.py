from dotenv import load_dotenv
import os

load_dotenv('.env')

LOADING_FILE = os.getenv("LOADING_FILE")

DATA_DIR = os.getenv("DATA_DIR")

LINK = os.getenv("LINK")

MONGOUSER = os.getenv("MONGOUSER")

MONGOPWD = os.getenv("MONGOPWD")

MONGOURL = os.getenv("MONGOURL")
MONGOPORT = os.getenv("MONGOPORT")
MONGOURI = [":".join([MONGOURL, MONGOPORT])]

DATABASENAME = os.getenv("DATABASENAME") or ".".join(
    [LOADING_FILE.split('/')[-1].split('.')[0], "db"])
