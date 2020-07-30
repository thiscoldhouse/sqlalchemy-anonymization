import hashlib
import random
from sqlalchemy import create_engine, MetaData, Table

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
            'You must register a SQLAlchemy Base object'
        )
    # create new schema
    # TODO: ensure that DB is empty
    target_engine = create_engine(target_db_uri)
    target_metadata = MetaData(bind=target_engine)

    # go through each table, create it, then copy and anonymize
    # the data
    for src_table in BASE.__subclasses__():
        print(src_table)
