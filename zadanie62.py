#part1 connection

from sqlalchemy import create_engine

# Create engine connection to SQLite database
engine = create_engine('sqlite:///zadanie62.db')

# Print engine's driver and table names
print(engine.driver)
print(engine.table_names())

# Execute a SELECT query on 'clean' table and print results
results = engine.execute("SELECT * FROM clean")
for r in results:
    print(r)

# Using sqlite3 to create a connection to the database
import sqlite3

db_file = "zadanie62.db"

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to {db_file}, sqlite version: {sqlite3.sqlite_version}")
    except sqlite3.Error as e:
        print(e)
    return conn

conn = create_connection(db_file)
#part2 tables creation

from sqlalchemy import Table, Column, Integer, String, MetaData

# Create engine
engine = create_engine('sqlite:///zadanie62.db')

# Define metadata
meta = MetaData()

# Define clean_stations table
clean_stations = Table(
    'clean_stations', meta,
    Column('station', String, primary_key=True),
    Column('latitude', Integer),
    Column('longitude', Integer),
    Column('elevation', Integer),
    Column('name', String),
    Column('country', String),
    Column('state', String),
)

# Define clean_measure table
clean_measure = Table(
    'clean_measure', meta,
    Column('station', String, primary_key=True),
    Column('date', String),
    Column('precip', Integer),
    Column('tobs', Integer),
)

# Create tables in the database
meta.create_all(engine)

# Print tables in the database
print(engine.table_names())


meta.create_all(engine)
print(engine.table_names())





#part3 insert

import csv
import sqlite3

def insert_stations():
    conn = sqlite3.connect('zadanie62.db')
    file = open('clean_stations.csv')
    contents = csv.reader(file)
    cur = conn.cursor()
    
    insert_records = "INSERT INTO clean_stations (station, latitude, longitude, elevation, name, country, state) VALUES(?, ?, ?, ?, ?, ?, ?)"
    cur.executemany(insert_records, contents)
    
    conn.commit()
    conn.close()

def insert_measure():
    conn = sqlite3.connect('zadanie62.db')
    measures = open('clean_measure.csv')
    rows = csv.reader(measures)
    cur = conn.cursor()
    
    insert_measures = "INSERT INTO clean_measure (station, date, precip, tobs) VALUES(?, ?, ?, ?)"
    cur.executemany(insert_measures, rows)
    
    conn.commit()
    conn.close()














