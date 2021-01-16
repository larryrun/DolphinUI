from sqlalchemy import Column, String, DateTime, Integer
from sqlalchemy.dialects.mysql import FLOAT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AuditEventTable(Base):

    __tablename__ = 'AuditEvent'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime(), index=True)
    ip = Column(String(128), index=True)
    event = Column(String(2048))

    @staticmethod
    def name():
        return AuditEventTable.__tablename__


