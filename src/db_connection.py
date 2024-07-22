import psycopg2

def get_connection():
    Postgres_local = psycopg2.connect(
        dbname="Postgres_local",
        user="admin",
        password="Hing3H3@lth",
        host="localhost",
        port="5432"
    )
    return Postgres_local
