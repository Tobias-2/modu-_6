
import sqlite3

def create_connection(db_file):
   """ create a database connection to the SQLite database
       specified by db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except sqlite3.Error as e:
       print(e)
   return conn

def add_project(conn, project):
   """
   Create a new project into the projects table
   :param conn:
   :param project:
   :return: project id
   """
   sql = '''INSERT INTO projects(nazwa, start_date, end_date)
             VALUES(?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, project)
   conn.commit()
   return cur.lastrowid

def select_all(conn, table):
   """
   Query all rows in the table
   :param conn: the Connection object
   :return:
   """
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows


def select_task_by_status(conn, status):
   """
   Query tasks by priority
   :param conn: the Connection object
   :param status:
   :return:
   """
   cur = conn.cursor()
   cur.execute("SELECT * FROM tasks WHERE status=?", (status,))

   rows = cur.fetchall()
   return rows

from sqlite3 import Error

def execute_sql(conn, sql):
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)


conn = create_connection("database.db")
cur = conn.cursor()
cur.execute("SELECT * FROM tasks")
rows = cur.fetchall()
print(rows)



def select_task_by_status(conn, status):
   """
   Query tasks by priority
   :param conn: the Connection object
   :param status:
   :return:
   """
   cur = conn.cursor()
   cur.execute("SELECT * FROM tasks WHERE status=?", (status,))

   rows = cur.fetchall()
   return rows



def select_all(conn, table):
   """
   Query all rows in the table
   :param conn: the Connection object
   :return:
   """
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows


conn = create_connection("database.db")
select_all(conn, "projects")
select_all(conn, "tasks")
select_where(conn, "tasks", project_id=1)
select_where(conn, "tasks", status="ended")