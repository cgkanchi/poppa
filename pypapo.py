import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy.sql import select, column, text, and_
from sqlalchemy.dialects import postgresql
import random
from string import ascii_lowercase
import itertools

# reconcile unicode and bytes and str for cross-py support
TYPE_MAPPER = {int: 'INTEGER', np.int64: 'INTEGER', np.int32: 'INTEGER', np.object_: 'TEXT', object: 'TEXT',
               np.float32: 'REAL', np.float64: 'DOUBLE PRECISION', float: 'DOUBLE PRECISION',
               str: 'TEXT', bytes: 'TEXT', np.datetime64: 'TIMESTAMPTZ', datetime: 'TIMESTAMPTZ', bool: 'BOOLEAN'}


DB_DEBUG = False


class PaPoDataFrame(object):
    '''PostgreSQL-backed DataFrame-like class. Allows arbitrary PostgreSQL queries to be run against data and provides
    convenience methods to load data into tables. Allows results of a query to be retrieved into a pandas DataFrame'''

    def __init__(self, name, connection, cursor=None, columns=None):
        self.name = name
        self.conn = connection
        self.cursor = cursor or self.conn.cursor()
        self.columns = columns

    def _exec_sql(self, sql, method=None, *args, **kwargs):
        if not method:
            method = self.cursor.execute

        method(sql, *args, **kwargs)
        if DB_DEBUG:
            print(self.cursor.query)

    def apply_to_col(self, col, func, *args, **kwargs):
        coldata = self.query(columns=[self.columns[col]])
        coldata[col].apply(func, *args, **kwargs)
        return coldata[col]

    def read_csv(self, csvfile, nrows=10, persist=False, drop=False, indexes=None, header='infer', *args, **kwargs):
        '''Load a CSV file into postgres into a temp (optionally not a temp) table and set appropriate indexes'''
        parse_dates = kwargs.pop('parse_dates', True)
        if drop:
            self._exec_sql('DROP TABLE IF EXISTS "{}"'.format(self.name))
        # take a small slice of the csv file and load into a pandas dataframe - then infer types and map to relevant PG types
        # note that arbitrary objects are NOT supported
        prefix = 'anon_' if header is None else None
        df = pd.read_csv(csvfile, nrows=nrows, prefix=prefix, parse_dates=parse_dates, *args, **kwargs)

        try:
            col_types = [('_index_', 'BIGSERIAL')] + [(c.replace(' ', '_'), TYPE_MAPPER[df[c].dtype.type]) for c in df.columns]
        except KeyError as ex:
            print('Found one or more invalid types')
            raise TypeError(ex.message)

        col_spec = ','.join(' '.join(cspec) for cspec in col_types)
        persist = 'TEMP' if not persist else ''
        # TODO put primary key here
        create_table = 'CREATE {persist} TABLE IF NOT EXISTS "{tblname}" ({col_spec})'.format(persist=persist, tblname=self.name, col_spec=col_spec)
        self._exec_sql(create_table)
        self._exec_sql('DELETE FROM "{tblname}"'.format(tblname=self.name))
        header = 'HEADER' if header is not None else ''
        copy_stmt = 'COPY "{tblname}" ({colnames}) FROM STDIN CSV {header}'.format(tblname=self.name, colnames=','.join([c[0] for c in col_types][1:]), csvfile=csvfile, header=header)
        with open(csvfile) as f:
            self._exec_sql(copy_stmt, self.cursor.copy_expert, f)

        indexes = indexes or df.columns
        for c in indexes:
            self._exec_sql('CREATE INDEX ON {}({})'.format(self.name, c))

        self.conn.commit()

        # create columns to allow querying
        self.columns = {c: column(c) for c in df.columns}


    def __getitem__(self, key):
        return self.columns[key]


    def __setitem__(self, key, value):
        col = column(key) if key not in self.columns else self.columns[key]
        value = pd.DataFrame(value).reset_index()
        value.columns = ['_index_', 'temp_val']
        base_value = '({}, {})'
        values = []
        for i, v in value.iterrows():
            values.append(base_value.format(v['_index_'], v['temp_val']))
        values = ','.join(values)

        if key not in self.columns:
            self._exec_sql('ALTER TABLE "{tblname}" ADD COLUMN {colname} {coltype}'.format(tblname=self.name, colname=key, coltype=TYPE_MAPPER[value['temp_val'].dtype.type]))
        self._exec_sql('''UPDATE "{tblname}" AS t SET {colname} = c.{colname} FROM (VALUES {values})
                          AS c(_index_, {colname}) WHERE c._index_ = t._index_'''.format(tblname=self.name, colname=key, values=values))
        if key not in self.columns:
            self._exec_sql('CREATE INDEX ON "{tblname}" ({colname})'.format(tblname=self.name, colname=key))

        self.columns.update(key=col)

    def _translate_query(self, query=None, columns=None, joins=None):
        if columns is not None:
            if '_index_' not in columns:
                columns = [column('_index_')] + columns
        columns = columns or [text('*')]

        if not isinstance(query, list) and not isinstance(query, tuple) and query is not None:
            query = [query]
        q = select(columns).select_from(text(self.name))
        if joins:
            q = q.join(*joins)
        if query:
            q = q.where(and_(*query))
        q = q.compile(dialect=postgresql.dialect(param_style='named'))
        return q.__str__(), q.params


    def _get_rand_str(self, nchars=30):
        return ''.join(itertools.chain.from_iterable(random.sample(ascii_lowercase, 1) for i in range(nchars)))

    def query(self, query=None, columns=None, group_by=None, as_temp=False):
        '''Query a table. If as_temp is True, put results into a temp table instead and return a PaPoDF on that temp table instead'''
        q, p = self._translate_query(query, columns)
        if as_temp:
            temp_table_name = self._get_rand_str()
            q = 'CREATE TEMP TABLE "{tblname}" AS ({query})'.format(tblname=temp_table_name, query=q)
        self._exec_sql(q, None, p)

        if as_temp:
            return PaPoDataFrame(temp_table_name, self.conn, self.cursor, self._get_columns_from_db(temp_table_name))
        else:
            df = pd.DataFrame(self.cursor.fetchall())
            df.columns = [c.name for c in self.cursor.description]
            return df

    def _get_columns_from_db(self, tblname):
        self.cursor.execute('SELECT * FROM "{tblname}" LIMIT 1'.format(tblname=tblname))
        self.cursor.fetchall()
        return {c.name: column(c.name) for c in self.cursor.description}




# from psycopg2 import connect
# c = connect('postgres://idatalabs:idata123@localhost/dfr_pg')
# p = PaPoDataFrame('test1', c)
# p.read_csv('/home/cgkanchi/test.csv')
# p2 = p.query([p['last_crawl_date'] > '2016-05-01'], as_temp=True)

