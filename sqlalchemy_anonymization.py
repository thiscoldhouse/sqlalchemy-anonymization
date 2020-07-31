import hashlib
import random
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey
import sys

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
    return hashlib.sha256(original_value.encode('utf-8')).hexdigest()

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

    # go through each table and create new target version
    src_tables = BASE.__subclasses__()
    target_tables = []
    while len(src_tables) > 0:
        # this lets us come back to do foreign key relationship
        skipped_tables = []
        for src_table in src_tables:
            columns = []
            for column in src_table.__table__.columns:
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
                     raise NotImplementedError()
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
                         skipped_tables.append(src_table)
                         break
            else:
                table = Table(
                    src_table.__tablename__,
                    target_metadata,
                    *columns
                )
                table.create()
                target_tables.append(table)

        if len(skipped_tables) == len(src_tables):
            raise Exception('Something has gone wrong')

        src_tables = skipped_tables[:]
        skipped_tables = []

    # we go through similar logic as above to generate and
    # insert the anonymized data.
    src_tables = BASE.__subclasses__()
    filled_tables = []
    filled_tables_length = 0  # check to avoid infinite looping
    while len(filled_tables) < len(src_tables):
        for src_table in src_tables:
            columns = []
            for column in src_table.__table__.columns:
                # check to make sure all foreign keys are
                # alreayd filled in, if not, skip and
                # do again later
                 if len(column.foreign_keys) == 0:
                     pass
                 elif len(column.foreign_keys) > 1:
                     raise NotImplementedError()
                 else:
                     fk = list(column.foreign_keys)[0]
                     if fk.column.table.name not in [
                             t.description
                             for t in filled_tables
                     ]:
                         # we'll do this later, missing parent
                         break
            else:
                filled_tables.append(
                    copy_and_anonymize(
                        src_table,
                        target_tables,
                        target_engine,
                    )
                )

        if len(filled_tables) == filled_tables_length:
            raise Exception('Something has gone wrong')
        else:
            filled_tables_length = len(filled_tables)

def copy_and_anonymize(src_table, target_tables, engine):
    target_table = None
    for table in target_tables:
        if table.description == src_table.__tablename__:
            target_table = table
            break
    else:
        raise ValueError(
            'Could not find target table for {}'.format(
                src_table.__tablename__
            )
        )

    anon_fxn_mapper = getattr(src_table, 'anonymize', {})
    for instance in src_table.query.all():
        insert_kwargs = {}
        for colname in src_table.__table__.columns.keys():
            if src_table.__table__.columns[colname].primary_key:
                # skip primary keys
                continue
            # handle foreign keys
            insert_kwargs[colname] = getattr(instance, colname)

            # anonymize if relevant
            if colname in anon_fxn_mapper.keys():
                insert_kwargs[colname] = anonymizers[
                    anon_fxn_mapper[colname]
                ](
                    insert_kwargs[colname]
                )
        engine.execute(target_table.insert(), **insert_kwargs)
    return target_table
