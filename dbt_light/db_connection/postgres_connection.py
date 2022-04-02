from typing import TextIO
import psycopg2


class PostgresConnection:
    def __init__(self, config: dict):
        self._conn = psycopg2.connect(host=config['host'],
                                      port=config['port'],
                                      database=config['dbname'],
                                      user=config['user'],
                                      password=config['pass'])
        self._cursor = self._conn.cursor()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self, commit: bool = True) -> None:
        if commit:
            self.commit()
        self.cursor.close()
        self.connection.close()

    def execute(self, sql: str, params: tuple = None) -> None:
        self.cursor.execute(sql, params or ())

    def fetchall(self) -> list:
        return self.cursor.fetchall()

    def fetchone(self) -> list:
        return self.cursor.fetchone()

    def query(self, sql: str, params: tuple = None) -> list:
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    def copy(self, command: str, file: TextIO):
        self.cursor.copy_expert(command, file)
