import psycopg2
from config import config

def postgresdb_connect(query='Select version()'):
    connection=None
    try:
        params=config()
        print('Connecting to Postgresql db..')
        connection= psycopg2.connect(**params)
        crsr=connection.cursor()
        print('Postgres database connection:')
        crsr.execute('Select version()')
        db_version=crsr.fetchone()
        print(db_version)
        crsr.close()
    except(Exception, psycopg2.DatabaseError ) as error:
        print(error)
    finally:
        if connection:
            connection.close()
            print('Db connection terminated')



