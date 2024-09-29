In [1]: from ex_04_selecty import *

In [2]: conn = create_connection("database.db")

In [3]: # wszystkie projekty

In [4]: select_all(conn, "projects")
Out[4]:
[(1, 'Pompki', '2020-05-08 00:00:00', '2020-05-10 00:00:00'),
 (2, 'Powtórka z angielskiego', '2020-05-11 00:00:00', '2020-05-13 00:00:00')]

In [5]: # wszystkie zadania

In [6]: select_all(conn, "tasks")
Out[6]:
[(1,
  2,
  'Czasowniki regularne',
  'Zapamiętaj czasowniki ze strony 30',
  'ended',
  '2020-05-11 12:00:00',
  '2020-05-11 15:00:00'),
 (2,
  2,
  'Czasowniki regularne',
  'Zapamiętaj czasowniki ze strony 31',
  'started',
  '2020-05-11 12:00:00',
  '2020-05-11 15:00:00'),
 (3,
  1,
  '100 pompek',
  '4 x 25',
  'started',
  '2020-05-12 12:00:00',
  '2020-05-12 15:00:00')]

In [7]: # wszystkie zadania dla projektu o id 1

In [8]: select_where(conn, "tasks", project_id=1)
Out[8]:
[(3,
  1,
  '100 pompek',
  '4 x 25',
  'started',
  '2020-05-12 12:00:00',
  '2020-05-12 15:00:00')]

In [9]: # wszystkie zadania ze statusem ended

In [10]: select_where(conn, "tasks", status="ended")
Out[10]:
[(1,
  2,
  'Czasowniki regularne',
  'Zapamiętaj czasowniki ze strony 30',
  'ended',
  '2020-05-11 12:00:00',
  '2020-05-11 15:00:00')]