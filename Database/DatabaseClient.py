from Database.DatabaseEngine import DatabaseEngine
from Database.DatabaseSchema import AuditEventTable
import pandas as pd


class DatabaseClient(object):

    def __init__(self):
        self.engine = DatabaseEngine()

    def get_engine(self):
        return self.engine.get_engine()

    def write_audit_event(self, audit_event):
        audit_event.to_sql(AuditEventTable.name(), self.engine.get_engine(), if_exists='append', index=False)

