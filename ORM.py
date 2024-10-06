from sqlalchemy import create_engine

engine = create_engine('sqlite:///database.db')

print(engine.driver)

print(engine.table_names())

print(engine.execute("SELECT * FROM tasks"))

results = engine.execute("SELECT * FROM tasks")

for r in results:
   print(r)


# sqlalchemy_ex_02.py
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import create_engine

engine = create_engine('sqlite:///database.db')

meta = MetaData()

students = Table(
   'students', meta,
   Column('id', Integer, primary_key=True),
   Column('name', String),
   Column('lastname', String),
)

meta.create_all(engine)
print(engine.table_names())




ins = students.insert()
ins
str(ins)
ins.compile().params
ins = students.insert().values(name='Eric', lastname='Idle')
ins.compile().params


ins = students.insert()

ins = students.insert().values(name='Eric', lastname='Idle')

conn = engine.connect()
result = conn.execute(ins)
conn.execute(ins, [
   {'name': 'John', 'lastname': 'Cleese'},
   {'name': 'Graham', 'lastname': 'Chapman'},
])



from sqlalchemy import create_engine, MetaData, Integer, String, Table, Column

engine = create_engine('sqlite:///database.db', echo=True)

meta = MetaData()

students = Table(
   'students', meta,
   Column('id', Integer, primary_key=True),
   Column('name', String),
   Column('lastname', String),
)

conn = engine.connect()
s = students.select().where(students.c.id > 2)
result = conn.execute(s)

for row in result:
   print(row)