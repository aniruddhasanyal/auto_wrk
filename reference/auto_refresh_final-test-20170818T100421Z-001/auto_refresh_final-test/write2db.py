import pandas as pd
import pyodbc
import config


class Write2DB:
    def __init__(self):
        self.conf = config.Config()
        self.connection_str = self.conf.get_db_conn(out_db=True)

    def _chunk(self, seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))


    def write_df(self, data=None, table='', columns=[]):
        if data is None:
            print('Please provide a data to write')
            exit(1)

        cnxn = pyodbc.connect(self.connection_str)
        cursor = cnxn.cursor()

        columns = []

        sql = "insert into {} ({}) values".format(table, ','.join(columns))
        records = [str(tuple(x)) for x in data.values]
        cursor.execute("Truncate table {}".format(table))
        items = self._chunk(records, 1000)
        for batch in items:
            rows = ','.join(batch)
            insert_rows = sql + rows
            cursor.execute(insert_rows)
        cnxn.commit()
        cnxn.close()


