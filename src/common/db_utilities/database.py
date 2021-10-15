# from credentials import DATASOURCES
import pyodbc
import re

# SQL_SERVER_DRIVERS = [r"^SQL Server$", r"^ODBC Driver [0-9]+ for SQL Server$"]
# sql_server_odbc_patterns = [re.compile(pattern) for pattern in SQL_SERVER_DRIVERS]


class Database:
    def __init__(self, label):
        self.label = label
        # self._env = env
        self._connection_string = None

    @property
    def connection_string(self):
        if self._connection_string:
            return self._connection_string

        self._connection_string = self._get_connection_string()
        return self._connection_string

    def _get_connection_string(self):
        raise NotImplementedError("Make sure this method is implemented.")

    # @property
    # def env(self):
    #     return self._env


class PostgreSQL(Database):
    def __init__(self, label, hostname, port, database, username, password=None):
        super().__init__(label)

        self.hostname = hostname
        self.port = port
        self.database = database
        self.username = username
        self._password = password

        self.validate()

    def _get_connection_string(self):
        return f"dbname={self.database} user={self.username} password={self._password} host={self.hostname} port={self.port}"

    def validate(self):
        required_fields = ["hostname", "port", "database", "username"]

        for field in required_fields:
            if not getattr(self, field):
                raise ValueError(f"Field={field} must hold a value")

    def __repr__(self):
        return f"<{self.__class__.__name__} Server={self.hostname}:{self.port}, Database={self.database}>"
