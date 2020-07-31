# This example is made with flask-sqlalchemy, which is how
# I normally see sql alchemy used in the wild. So long as
# you have a base model to register you can use plain old
# SQLAlchemy.

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import random

from sqlalchemy_anonymization import (
    register_base,
    anonymizer,
    create_anonymized_database
)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/example.db'
db = SQLAlchemy(app)

class BaseModel(db.Model):
    __abstract__ = True

BaseModel = register_base(BaseModel)

# a custom anonymizer function
@anonymizer
def custom_phone_number(original_value):
    return str(random.randint(1111111111, 9999999999))

class Emperor(BaseModel):
    __tablename__ = 'emperor'
    emperor_id = db.Column('emperor_id', db.Integer, primary_key=True)
    name =  db.Column('name', db.Text)
    phone_number = db.Column('phone_number', db.Text)

    anonymize = {
        'name': 'sha256',  # included in lib
        'phone_number': 'custom_phone_number'  # written above
    }

class Battle(BaseModel):
    __tablename__ = 'battle'
    battle_id = db.Column('battle_id', db.Integer, primary_key=True)
    name = db.Column('name', db.Text)
    success = db.Column('success', db.Boolean)
    emperor_id = db.Column(
        'emperor_id',
        db.Integer,
        db.ForeignKey('emperor.emperor_id')
    )
    anonymize = {
        'name': 'sha256',  # included in lib
        'success': 'boolean' # included in lib
    }

db.create_all()

augustus = Emperor(name='Augustus', phone_number='2222222222')
db.session.add(augustus)
db.session.flush()
actium = Battle(name='Actium', success=True, emperor_id=augustus.emperor_id)
teutoburg = Battle(name='Teutoburg', success=False, emperor_id=augustus.emperor_id)
db.session.add(actium)
db.session.add(teutoburg)
db.session.commit()

create_anonymized_database('sqlite://///tmp/fakedata.sql')
