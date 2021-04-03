import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey


def connect(user, password, db, host='localhost', port=5432):
    '''Returns a connection and a metadata object'''
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    print(url)
    con = sqlalchemy.create_engine(url, client_encoding='utf8')
    meta = sqlalchemy.MetaData(bind=con)

    return con, meta


# Press the green button in the gutter to run the script.
con, meta = connect('postgres', 'admin', 'stock')

slams = Table('slams', meta,
              Column('name', String, primary_key=True),
              Column('country', String)
              )

results = Table('results', meta,
                Column('slam', String, ForeignKey('slams.name')),
                Column('year', Integer),
                Column('result', String)
                )

# Create the above tables
meta.create_all(con)