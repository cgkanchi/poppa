# poppa
A Python library that leverages PostgreSQL to solve medium-data problems

## Introduction

PoPPa is a Python library that attempts to act as a bridge between PostgreSQL and Pandas. PoPPa attempts to perform as many data operations and transforms as possible in Postgres, to prevent the need to load data directly into RAM. In some ways, PoPPa is similar to Blaze. However, while Blaze attempts to maintain compatibility with several backends using SQLAlchemy, PoPPa unabashedly uses Postgres-specific features that are missing/not implemented in SQLAlchemy (sometimes via dirty hacks). In particular, PoPPa makes extensive use of TEMP TABLEs to store intermediate results of operations. In general, most, if not all PoppaDataFrame objects are backed by a table (temporary or not) in Postgres.

PoPPa is currently in a pre-alpha state and is _very_ incomplete, untested and (probably) very buggy. Use at your own risk.


Example:


    import poppa
    import psycopg2

    conn = psycopg2.connect('postgres://username:password@host/db')

    pdf = poppa.PoppaDataFrame(name='test1', connection=conn)
    pdf.read_csv('/path/to/csv')   # this creates a temp table within postgres
    pdf2 = pdf.query(pdf['some_column'] > 50, as_temp=True)  # select a subset of data into a second PoppaDataFrame
    pdf2['some_other_column'] = pdf2.query(columns=['some_column'])['some_column'] * 2  # add a column on the fly

    pandas_df = pdf2.query(pdf2['some_column_2'] == 4)  # returns a pandas DataFrame
