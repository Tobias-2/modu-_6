import sqlite3
from sqlite3 import Error

db_file = "zadanie_61.db"

def create_connection(db_file):
    
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to {db_file}, sqlite version: {sqlite3.sqlite_version}")
    except Error as e:
        print(e)
    return conn

def create_connection_in_memory():
    
    conn = None
    try:
        conn = sqlite3.connect(":memory:")
        print(f"Connected to in-memory database, sqlite version: {sqlite3.sqlite_version}")
    except Error as e:
        print(e)
    return conn

def execute_sql(conn, sql):
    
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

def add_tabela1(conn, tabela1):
    
    sql = '''INSERT INTO tabela1(nazwa_produktu, production_date, expiration_date)
             VALUES(?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, tabela1)
    conn.commit()
    return cur.lastrowid

def add_tabela2(conn, tabela2):
    
    sql = '''INSERT INTO tabela2(product_id, nazwa_produktu, opis, stan_na_sklepie, production_date, expiration_date)
             VALUES(?,?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, tabela2)
    conn.commit()
    return cur.lastrowid

if __name__ == "__main__":
    # Create tables
    create_tabela1_sql = """
    CREATE TABLE IF NOT EXISTS tabela1 (
        id integer PRIMARY KEY,
        nazwa_produktu text NOT NULL,
        production_date text,
        expiration_date text
    );"""

    create_tabela2_sql = """
    CREATE TABLE IF NOT EXISTS tabela2 (
        id integer PRIMARY KEY,
        product_id integer NOT NULL,
        nazwa_produktu VARCHAR(250) NOT NULL,
        opis TEXT,
        stan_na_sklepie VARCHAR(15) NOT NULL,
        production_date text,
        expiration_date text,
        FOREIGN KEY (product_id) REFERENCES projects (id)
    );"""

    conn = create_connection(db_file)

    if conn is not None:
        execute_sql(conn, create_tabela1_sql)
        execute_sql(conn, create_tabela2_sql)

        # Insert data
        tabela1 = ("Masło", "2020-05-11 00:00:00", "2020-07-13 00:00:00")
        t1_id = add_tabela1(conn, tabela1)

        tabela2 = (
            t1_id,
            "Papryka",
            "Za dużo",
            "started",
            "2020-05-11 12:00:00",
            "2020-05-17 15:00:00"
        )
        t2_id = add_tabela2(conn, tabela2)

        print(t1_id, t2_id)
        
        conn.close()


#READ

conn = create_connection("zadanie_61.db")
cur = conn.cursor()
cur.execute("SELECT * FROM tabela2")
rows = cur.fetchall()
print(rows)


#UPDATE


def update(conn, table, id, **kwargs):
   
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)

if __name__ == "__main__":
   conn = create_connection("zadanie_61.db")
   update(conn, "tabela2", 2, stan_na_sklepie="Nie ma")
   update(conn, "tabela2", 2, opis="smaczne")
   conn.close()


#DELTE

def delete_where(conn, table, **kwargs):
   
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)

   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Deleted")

def delete_all(conn, table):
   
   sql = f'DELETE FROM {table}'
   cur = conn.cursor()
   cur.execute(sql)
   conn.commit()
   print("Deleted")

if __name__ == "__main__":
   conn = create_connection("zadanie_61.db")
   delete_where(conn, "tabela2", id=3)
   delete_all(conn, "tabela1")