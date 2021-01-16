from Database.DatabaseSchema import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class DatabaseEngine(object):

    def __init__(self):
        #engine = create_engine('mysql+pymysql://root:pwd4root@localhost/Dolphin', encoding='utf-8', echo=True)
        engine = create_engine('mysql+pymysql://root:pwd4root@sh-cdb-1m56f2js.sql.tencentcdb.com:60388/Dolphin', encoding='utf-8', echo=True)
        Base.metadata.create_all(engine)
        session = sessionmaker()
        session.configure(bind=engine)
        self.engine = engine
        self.session = session

    def get_engine(self):
        return self.engine

    def get_session(self):
        return self.session
