from peewee import *
from playhouse.pool import PooledSqliteDatabase
from generate_data import task_runner
# import logging
import concurrent.futures
import settings

# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

def get_name():
   return '/'.join([settings.DATA_DIR, settings.DATABASENAME])

db = PooledSqliteDatabase(get_name(), max_connections=32,
                          pragmas={
                              'journal_mode': 'wal',
                              'cache_size': -1 * 512000,  # 64MB
                              'foreign_keys': 1,
                              'ignore_check_constraints': 0,
                              'synchronous': 0})


class dbBuild:
    @classmethod
    def populate_store(cls, values, *arg, **kwargs):
        index = Carrier.select().count()
        task_runner(cls.create_objects_atomic, values, index,
                    chunk_size=1000, *arg, **kwargs)

    @classmethod
    def create_objects_atomic(cls, objs, *arg, **kwargs):
        # logging.info('atomic')
        db.connect(reuse_if_open=True)
        cls.create_objects(objs)
        db.close()

    @classmethod
    def create_objects(cls, items):
        carrier = []
        freight = []
        vehicles = []
        for i in items:
            car, fre, veh = cls.build_object(i)
            carrier.append(car)
            freight.extend(fre)
            vehicles.extend(veh)
        Carrier.bulk_create(carrier)
        CarrierFreight.bulk_create(freight)
        CarrierVehicles.bulk_create(vehicles)

    @staticmethod
    def build_object(item):
        carrier_id = item.get('carrier_id')
        carrier = Carrier(id=carrier_id)
        freight = [CarrierFreight(value=v, freighttype=k, carrier_id=carrier)
                    for k, v in item['cargo'].items()]
        vehicles = []
        for vtype in item['types']:
            val = vtype.pop('vehicle_type')
            vehicles.append(CarrierVehicles(vehicletype=val,
                                            carrier_id=carrier_id, **vtype))
        return carrier, freight, vehicles


class BaseModel(Model):
    class Meta:
        database = db


class Carrier(BaseModel):
    id = IntegerField(primary_key=True)


class CarrierVehicles(BaseModel):
    owned = IntegerField()
    term_leased = IntegerField()
    trip_leased = IntegerField()
    carrier = ForeignKeyField(Carrier, backref='vehicles')
    vehicletype = CharField()

    class Meta:
        indexes = (
            (('carrier', 'vehicletype'), True),
        )


class CarrierFreight(BaseModel):
    value = BooleanField()
    carrier = ForeignKeyField(Carrier, backref='freight')
    freighttype = CharField()

    class Meta:
        indexes = (
            (('carrier', 'freighttype'), True),
        )


def create_tables():
    with db:
        db.create_tables([Carrier, CarrierVehicles, CarrierFreight], safe=True)

create_tables()