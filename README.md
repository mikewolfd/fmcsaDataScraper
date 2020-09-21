# fmcsaDataScraper

This is a simple scraping tool to gather CarrierRegistration data from the ai.fmcsa.dot.gov website.

Data is accessible from mongodb or exported as an sqlite3 db file. 

The csv is loaded using a pandas dataframe, multiprocessing is handled by concurent.futures, data is parsed by beautifulsoup (lxml), and is scraped into mongodb. Peewee is used as an ORM for sqlite3, although this can easily be modified to use postgresql or mysql. I use docker-compose to run a mongo instance with a locally mounted dir. 

All values are striped of non-alphanumeric values, converted to snakecase, and numerical values are converted to ints.

Example mongo object shape: 

    {'carrier_id': 1, 'cargo': {'general_freight': False, ...}, 'types': [{'vehicle_type': 'hazmat_cargo_tank_trailers',...}, ...], 'index': 0}

The SQL models are Carrier, CarrierFreight, and CarrierVehicles, indexed in type, with foreignkeys to Carrier. Carrier's primary key is the carrier_id from the csv.

The two functions in main.py are 'scrape' and 'build_sql':

    'scrape' has multiple optional arguments:
        quantity: an indexed and sliced amount of data based on the .csv index, defaults to all
        max_tries: number of retry incase of connection or db access failure 
        cooldown: in minutes incase of retry
        max_workers: the number of process workers accessing the site and loading data 

    'build_sql' accepts optional arguments such as max_workers.

The file layout:
    
    main.py - convenience functions 
    settings.py - env loading, processing
    generate_data.py - multiProcessing, data management for mongodb loading, main entry points
    mongo_storage.py - pymongo entry with convinence functions
    sql_store.py - peewee/sqlite entry with models and object creation code
    scrape.py - url request, text processing, object shaping code


To use:

    Create a .env file using example.env with updated or modified settings.
    Install packages from requirements.txt
    Docker and docker-compose must be installed to use the bundled mongodb, to run use 'docker-compose up'

    In a python terminal or notebook, launch the scrape function from main.py to load the mongo db.

    If you believe there was a distruption or failed loading, run the fix_store function from generate_data.py, it will search mongodb for locked entries and attempt to reload them.

    Run the build_sql from main.py to generate a sqlite3 file in the datadir.

TODO:
    testing
    csv exporting
    finish dockerizing the package
    memory optimization for the multiprocessing
