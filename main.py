import psycopg2
from config import user, password, db_name


def delete_table(conn, name_table):
    with conn.cursor() as cur:
        cur.execute(
            f"""DROP TABLE {name_table};""")
        return f'Таблица {name_table} была удалена'    


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS clients(
               client_id SERIAL PRIMARY KEY,
               first_name VARCHAR(100) NOT NULL,
               last_name VARCHAR(100) NOT NULL,
               email VARCHAR(50) NOT NULL UNIQUE);""" 
        )

        cur.execute(
            """CREATE TABLE IF NOT EXISTS numbers(
               number_id SERIAL PRIMARY KEY,
               client_id INTEGER NOT NULL REFERENCES clients(client_id),
               number VARCHAR(50));""" 
        )
        return 'Таблицы созданы'


def add_new_client(conn, first_name, last_name, email, number=None):
    if number:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO clients(first_name, last_name, email)
                   VALUES(%s,%s ,%s );""", (first_name, last_name, email,)
            )

            cur.execute(
                """SELECT client_id 
                   FROM clients
                   WHERE email=%s;""", (email,))    
            cl_id = cur.fetchone()[0]         
        
            cur.execute(
                """INSERT INTO numbers(client_id, number)
                   VALUES(%s,%s);""", (cl_id, number)
            )
        return 'Клиент добавлен'
    else:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO clients(first_name, last_name, email)
                   VALUES(%s,%s ,%s );""", (first_name, last_name, email,)
            )
        return 'Клиент добавлен'       


def add_number(conn, client_id, number):
    all_clients = all_clients_id_list(conn)
    if client_id in all_clients:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO numbers(client_id, number)
                   VALUES(%s,%s);""", (client_id, number)
            )           
            return f'Номер {number} для клиента с id{client_id} добавлен'
    else:
        return 'Клиента с данным id нет!'        


def update_clients(conn, client_id, first_name=None, last_name=None, email=None, number=None):
    with conn.cursor() as cur:
        all_clients = all_clients_id_list(conn)
        if client_id in all_clients:
            cur.execute(
                """UPDATE clients
                   SET first_name=%s, last_name=%s, email=%s
                   WHERE client_id=%s;""", (first_name, last_name, email, client_id)
            )

            cur.execute(
                """UPDATE numbers
                   SET number=%s
                   WHERE client_id=%s;""", (number, client_id)
            )
            return f'По клиенту с id {client_id} были внесены все изменения!'
        else:
            return 'Клиента с данным id нет в таблице!'    


def delete_number(conn, client_id, number):
    all_clients = all_clients_id_list(conn)
    if client_id in all_clients:
        with conn.cursor() as cur:
            cur.execute(
                """DELETE FROM numbers
                   WHERE client_id=%s
                   AND number=%s;""", (client_id, number)
            )
        return f'{number} был удален у клиента с id {client_id}'
    else:
        return 'Клиента с данным id нет!'    


def all_clients_id_list(conn):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT *
               FROM clients;"""
        )
        all_clients = cur.fetchall()
        list_id_clients = [i[0] for i in all_clients]
        return list_id_clients


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        all_clients = all_clients_id_list(conn)
        if client_id in all_clients:
            cur.execute(
                """DELETE FROM numbers
                   WHERE client_id=%s;""", (client_id,)
            )
            cur.execute(
                """DELETE FROM clients
                   WHERE client_id=%s;""", (client_id,)
            )
            return f'Клиент с id {client_id} был удален!'
        else:
            return 'В таблице нет клиента с таким id!'    


def find_client(conn, first_name=None, last_name=None, email=None, number=None):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT c.client_id, first_name, last_name, email, number
               FROM clients
               AS c
               LEFT JOIN numbers AS n
               ON n.client_id = c.client_id
               WHERE c.first_name=%s 
               OR c.last_name=%s 
               OR c.email=%s 
               OR n.number=%s
               ;""", (first_name, last_name, email, number)
               
        )
        return cur.fetchall()

                
            

with psycopg2.connect(database=db_name, user=user, password=password) as conn:
    conn.autocommit = True
    print(delete_table(conn, 'numbers'))
    print(create_table(conn))
    print(add_new_client(conn, 'Mikhail32', 'Abakumov123', '1234@ya.ru', '2239124711231238' ))
    print(add_number(conn, 12, '890212331412'))
    print(update_clients(conn, 9, first_name='baba', last_name='yaga', email='ddd@ya.ru', number='312414124'))
    print(delete_number(conn, 1, '8900000000'))
    print(delete_client(conn, 9))
    print(find_client(conn, last_name='Abakumov2'))
conn.close()
