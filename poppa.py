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


DB_DEBUG = True


class PoppaDataFrame(object):
    '''PostgreSQL-backed DataFrame-like class. Allows arbitrary PostgreSQL queries to be run against data and provides
    convenience methods to load data into tables. Allows results of a query to be retrieved into a pandas DataFrame'''

    def __init__(self, name, connection, columns=None):
        self.name = name
        self.conn = connection
        self.conn.autocommit = True
        self.columns = columns

    def _exec_sql(self, sql, cursor, *args, **kwargs):
        try:
            cursor.execute(sql, *args, **kwargs)
        finally:
            if DB_DEBUG:
                print(cursor.query)
        return cursor

    def apply_to_col(self, col, func, *args, **kwargs):
        coldata = self.query(columns=[self.columns[col]])
        coldata.set_index('_index_', inplace=True)
        return coldata[col].apply(func, *args, **kwargs)

    def persist(self, name=None):
        if not name:
            name = self.name

        tblname = self._get_rand_str() if name == self.name else name
        with self.conn:
            with self.conn.cursor() as cursor:
                self._exec_sql('CREATE TABLE "{}" AS (SELECT * FROM "{}")'.format(tblname, self.name), cursor)
                for c in self.columns:
                    self._exec_sql('CREATE INDEX ON {}({})'.format(tblname, c), cursor)
                if name == self.name:
                    self._exec_sql('DROP TABLE "{}"'.format(self.name), cursor)
                    self._exec_sql('ALTER TABLE "{}" RENAME TO "{}"'.format(tblname, self.name), cursor)
        self.conn.commit()


    def read_table(self):
        self.columns = self._get_columns_from_db(self.name)


    def read_csv(self, csvfile, nrows=10, persist=False, drop=False, indexes=None, header='infer', *args, **kwargs):
        '''Load a CSV file into postgres into a temp (optionally not a temp) table and set appropriate indexes'''
        parse_dates = kwargs.pop('parse_dates', True)
        if drop:
            with self.conn:
                with self.conn.cursor() as cursor:
                    self._exec_sql('DROP TABLE IF EXISTS "{}"'.format(self.name), cursor)
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
        with self.conn:
            with self.conn.cursor() as cursor:
                self._exec_sql(create_table, cursor)
                self._exec_sql('DELETE FROM "{tblname}"'.format(tblname=self.name), cursor)

        header = 'HEADER' if header is not None else ''
        copy_stmt = 'COPY "{tblname}" ({colnames}) FROM STDIN CSV {header}'.format(tblname=self.name, colnames=','.join([c[0] for c in col_types][1:]), csvfile=csvfile, header=header)
        with open(csvfile) as f:
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.copy_expert(copy_stmt, f)

        indexes = indexes or df.columns
        for c in indexes:
            with self.conn:
                with self.conn.cursor() as cursor:
                    self._exec_sql('CREATE INDEX ON {}({})'.format(self.name, c), cursor)

        self.conn.commit()

        # create columns to allow querying
        self.columns = {c: column(c) for c in df.columns}

    @staticmethod
    def _chunk_iter(iterable, chunksize):
        i = 0
        while True:
            chunk = iterable[i: i+ chunksize]
            i += chunksize
            if chunk:
                yield chunk
            else:
                raise StopIteration

    def __getitem__(self, key):
        return self.columns[key]


    def __setitem__(self, key, value):
        col = column(key) if key not in self.columns else self.columns[key]
        value = pd.DataFrame(value).reset_index()
        value.columns = ['_index_', 'temp_val']
        base_value = '({}, {})'
        values = []
        for i, v in value.iterrows():
            values.append(base_value.format('{}::bigint'.format(v['_index_']), v['temp_val']))

        if key not in self.columns:
            with self.conn:
                with self.conn.cursor() as cursor:
                    self._exec_sql('ALTER TABLE "{tblname}" ADD COLUMN {colname} {coltype}'.format(
                                   tblname=self.name, colname=key, coltype=TYPE_MAPPER[value['temp_val'].dtype.type]), cursor)

        for values_chunk in PoppaDataFrame._chunk_iter(values, 1000):
            values_chunk = ','.join(values_chunk)
            try:
                sql = '''UPDATE "{tblname}" AS t SET {colname} = c.{colname} FROM (VALUES {values})
                         AS c(_index_, {colname}) WHERE c._index_ = t._index_'''.format(tblname=self.name, colname=key, values=values_chunk)
                with self.conn:
                    with self.conn.cursor() as cursor:
                        self._exec_sql(sql, cursor)
            except Exception:
                print(sql)
                raise
        if key not in self.columns:
                with self.conn:
                    with self.conn.cursor() as cursor:
                        self._exec_sql('CREATE INDEX ON "{tblname}" ({colname})'.format(tblname=self.name, colname=key), cursor)

        self.columns.update({key: col})

    def _translate_query(self, query=None, columns=None, joins=None, distinct=None, no_index=False, fresh_index=False):
        if columns is not None:
            if '_index_' not in columns and not no_index:
                columns = [column('_index_')] + columns
        else:
            columns = [self.columns[c] for c in self.columns if c != '_index_']
            if '_index_' not in columns and not no_index:
                columns = [column('_index_')] + columns

        if not isinstance(query, list) and not isinstance(query, tuple) and query is not None:
            query = [query]
        q = select(columns).select_from(text(self.name))
        if distinct:
            q = q.distinct(*distinct)
        if joins:
            q = q.join(*joins)
        if query:
            q = q.where(and_(*query))
        q = q.compile(dialect=postgresql.dialect(param_style='named'))
        qstr = q.__str__()

        if fresh_index:
            # wrap query in SELECT row_number() call
            alias = self._get_rand_str()
            qstr = 'SELECT row_number() over (order by NULL) as _index_, * FROM ({q}) as {alias}'.format(q=qstr, alias=alias)

        return qstr, q.params


    def group_apply(self, group_by, func, col_subset, as_temp=True, *args, **kwargs):
        # first get all distincts for the group
        gb = [column(g) for g in group_by]
        group_df = self.query(columns=gb, distinct=gb, no_index=True, fresh_index=True)

        pdfs = []
        for i, row in group_df.iterrows():
            #TODO: parallelize
            # select each group into a temp table
            row_dict = row.to_dict()
            row_dict.pop('_index_')
            qcond = []
            for key in row_dict:
                qcond.append(column(key) == row_dict[key])

            pdfs.append(self.query(qcond, as_temp=True, no_index=True, fresh_index=True))
            self.conn.commit()
            for col in col_subset:
                pdfs[-1]['result_1'] = pdfs[-1].apply_to_col(col, func, *args, **kwargs)

        # first create a temp_table
        final_temp_name = self._get_rand_str()
        pdf = pdfs[0]
        with self.conn:
            with self.conn.cursor() as cursor:
                self._exec_sql('CREATE TEMP TABLE "{}" AS (SELECT * from "{}")'.format(final_temp_name, pdf.name), cursor)
                self.conn.commit()
                for i, pdf in enumerate(pdfs[1:]):
                    p_cols = ','.join(pdf.columns)
                    self._exec_sql('INSERT INTO "{tblname}" ({p_cols}) (SELECT {p_cols} FROM {tbl2})'.format(tblname=final_temp_name, p_cols=p_cols, tbl2=pdf.name), cursor)
                    self.conn.commit()

        final_pdf = PoppaDataFrame(name=final_temp_name, connection=self.conn, columns=self._get_columns_from_db(final_temp_name))
        final_pdf.reset_index()
        return final_pdf

    def reset_index(self):
        seq_name = '{}__index__seq'.format(self.name)

        with self.conn:
            with self.conn.cursor() as cursor:
                self._exec_sql('ALTER TABLE {} DROP COLUMN _index_'.format(self.name), cursor)
                self._exec_sql('ALTER TABLE {} ADD COLUMN _index_ BIGSERIAL'.format(self.name), cursor)
                self._exec_sql("SELECT setval('{}', 1, FALSE)".format(seq_name), cursor)
                self._exec_sql("UPDATE {} SET _index_=nextval('{}')".format(self.name, seq_name), cursor)

    def _get_rand_str(self, nchars=30):
        return ''.join(itertools.chain.from_iterable(random.sample(ascii_lowercase, 1) for i in range(nchars)))

    def query(self, query=None, columns=None, group_by=None, as_temp=False, distinct=None, no_index=False, fresh_index=False):
        '''Query a table. If as_temp is True, put results into a temp table instead and return a PaPoDF on that temp table instead'''
        q, p = self._translate_query(query, columns, distinct=distinct, no_index=no_index, fresh_index=fresh_index)
        if as_temp:
            temp_table_name = self._get_rand_str()
            q = 'CREATE TEMP TABLE "{tblname}" AS ({query})'.format(tblname=temp_table_name, query=q)
        with self.conn:
            with self.conn.cursor() as cursor:
                self._exec_sql(q, cursor, p)
                self.conn.commit()

                if as_temp:
                    return PoppaDataFrame(temp_table_name, self.conn, self._get_columns_from_db(temp_table_name))
                else:
                    df = pd.DataFrame(cursor.fetchall())
                    df.columns = [c.name for c in cursor.description]
                    return df

    def _get_columns_from_db(self, tblname):
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute('SELECT * FROM "{tblname}" LIMIT 1'.format(tblname=tblname))
                cursor.fetchall()
                return {c.name: column(c.name) for c in cursor.description}




# from psycopg2 import connect
# c = connect('postgres://idatalabs:idata123@localhost/dfr_pg')
# p = PaPoDataFrame('test1', c)
# p.read_csv('/home/cgkanchi/test.csv')
# p2 = p.query([p['last_crawl_date'] > '2016-05-01'], as_temp=True)

