import psycopg2
from config import user, password, db_name


def delete_table(cur, name_table):
    cur.execute(
        f"""DROP TABLE {name_table};""")
    return f'Таблица {name_table} была удалена'    

def create_table(cur):
    cur.execute(
        """CREATE TABLE IF NOT EXISTS clients(
           client_id SERIAL PRIMARY KEY,
           first_name VARCHAR(100),
           last_name VARCHAR(100),
           email VARCHAR(50) UNIQUE);""" 
    )

    cur.execute(
        """CREATE TABLE IF NOT EXISTS numbers(
           number_id SERIAL PRIMARY KEY,
           client_id INTEGER NOT NULL REFERENCES clients(client_id),
           number VARCHAR(50));""" 
    )
    return 'Таблицы созданы'

def add_new_client(cur, first_name, last_name, email, number=None):
    if number:
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
        
        cur.execute(
            """INSERT INTO clients(first_name, last_name, email)
               VALUES(%s,%s ,%s );""", (first_name, last_name, email,)
        )
        return 'Клиент добавлен'       

def add_number(cur, client_id, number):
    all_clients = all_clients_id_list(cur)
    if client_id in all_clients:
        cur.execute(
            """INSERT INTO numbers(client_id, number)
               VALUES(%s,%s);""", (client_id, number)
        )           
        return f'Номер {number} для клиента с id{client_id} добавлен'
    else:
        return 'Клиента с данным id нет!'        

def update_clients(cur, client_id, **values):
    all_clients = all_clients_id_list(cur)
    if client_id in all_clients:
        for key, value in values.items():
            cur.execute(
                f"""UPDATE clients
                    SET {key}='{value}'
                    WHERE client_id='{client_id}';"""
            )       
        return f'По клиенту с id {client_id} были внесены все изменения!'
    else:
        return 'Клиента с данным id нет в таблице!'    

def delete_number(cur, client_id, number):
    all_clients = all_clients_id_list(cur)
    if client_id in all_clients:
        cur.execute(
            """DELETE FROM numbers
               WHERE client_id=%s
               AND number=%s;""", (client_id, number)
        )
        return f'{number} был удален у клиента с id {client_id}'
    else:
        return 'Клиента с данным id нет!'    

def all_clients_id_list(cur):
    cur.execute(
        """SELECT *
           FROM clients;"""
    )
    all_clients = cur.fetchall()
    list_id_clients = [i[0] for i in all_clients]
    return list_id_clients

def delete_client(cur, client_id):
    all_clients = all_clients_id_list(cur)
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

def find_client(cur, **values):
    clients_find = []
    for key, value in values.items():
        cur.execute(
            f"""SELECT c.client_id, first_name, last_name, email, number
                FROM clients
                AS c
                LEFT JOIN numbers AS n
                ON n.client_id = c.client_id
                WHERE {key} = '{value}'
                ;"""          
            )
        clients_find.append(cur.fetchone())
    return clients_find


                
            
if __name__ == '__main__':
    with psycopg2.connect(database=db_name, user=user, password=password) as conn:
        with conn.cursor() as cur:
            conn.autocommit = True
            print(delete_table(cur, 'clients'))
            print(create_table(cur))
            print(add_new_client(cur, 'Mikhai', 'Abakumov112', '11@ya.ru', '223911231238' ))
            print(add_number(cur, 12, '890212331412'))
            print(update_clients(cur, 3, first_name='poluchilos', email='qw@fds'))
            print(delete_number(cur, 1, '8900000000'))
            print(delete_client(cur, 9))
            print(find_client(cur, email='12231@ya.ru', first_name='yra'))
    conn.close()
