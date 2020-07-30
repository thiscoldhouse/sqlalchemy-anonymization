import hashlib
import random
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey

from pprint import pprint

BASE = None  # gets filled in when sqlalchemy app is initialized
anonymizers = {}  # decorator fills this

# --------- anonymizers --------- #

def anonymizer(func):
    anonymizers[func.__name__] = func
    def inner():
        func()
    return inner

@anonymizer
def boolean(original_value):
    return random.choice((True, False))

@anonymizer
def sha256(original_value):
    return hashlib.sha256().update(original_value).hexdigest()

# --------- for registering in the sqlalchemy project ------- #

def register_base(base):
    global BASE
    BASE = base
    return base

# --------- generate anonymized data set ------ #

def create_anonymized_database(target_db_uri):
    """
    Expects a full DB URI, including credentials. Will make sure
    that DB is empty, then will create and insert an anonymzed
    version of the registered sqlalchemy DB.
    """
    if BASE is None:
        raise EnvironmentError(
            'You must register a SQLAlchemy base class'
        )
    # create new schema
    # TODO: ensure that DB is empty
    target_engine = create_engine(target_db_uri)
    target_metadata = MetaData(bind=target_engine)

    # go through each table, create it, then copy and anonymize
    # the data
    src_tables = BASE.__subclasses__()
    target_tables = []
    while len(src_tables) > 0:
        # this lets us come back to do foreign key relationship
        print(src_tables)
        skipped_tables = []
        for src_table in src_tables:
            columns = []
            for src_instance in src_table.query.all():
                 for column in src_instance.__table__.columns:
                     # handle columns with foreign keys differently
                     if len(column.foreign_keys) == 0:
                         columns.append(
                             Column(
                                 column.name,
                                 column.type,
                                 primary_key=column.primary_key,
                                 nullable=column.nullable
                             )
                         )
                     elif len(column.foreign_keys) > 1:
                         # to be honest I don't know what this
                         # means
                         raise NotImplemented
                     else:
                         fk = list(column.foreign_keys)[0]
                         if fk.column.table.name in [
                                 t.name for t in target_tables
                         ]:
                             columns.append(
                                 Column(
                                     column.name,
                                     column.type,
                                     ForeignKey(fk._colspec),
                                     primary_key=column.primary_key,
                                     nullable=column.nullable
                                 )
                             )

                         else:
                             # we'll do this later, missing parent
                             print('Skipping table with foreign key for now because parent does not exist yet')
                             skipped_tables.append(src_table)
                             break

            else:
                target_tables.append(
                    Table(
                        src_instance.__table__.name,
                        target_metadata,
                        *columns
                    )
                )
        if len(skipped_tables) == len(src_tables):
            raise Exception('Something has gone wrong')

        src_tables = skipped_tables[:]
        skipped_tables = []

    pprint(target_tables)

def create_new_table(metadata, columns_dict):
    """
    columns_dict should look like:
    {
        column_name {
            'type': SqlAlchemy type object,
            'primary_key': True/False,
            'nullable': True/False
        }
    }
    """
