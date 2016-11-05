# PoPPa
A Python library that leverages PostgreSQL to solve medium-data problems

## Introduction

PoPPa is a Python library that attempts to act as a bridge between PostgreSQL and Pandas. PoPPa attempts to perform as many data operations and transforms as possible in Postgres, to prevent the need to load data directly into RAM. In some ways, PoPPa is similar to Blaze. However, while Blaze attempts to maintain compatibility with several backends using SQLAlchemy, PoPPa unabashedly uses Postgres-specific features that are missing/not implemented in SQLAlchemy (sometimes via dirty hacks). In particular, PoPPa makes extensive use of TEMP TABLEs to store intermediate results of operations. In general, most, if not all PoppaDataFrame objects are backed by a table (temporary or not) in Postgres.

PoPPa is currently only compatible with Python 3.

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
    
    pdf2.persist(name='some_name')  # saves the calculation into a permanent table so you can resume

    # in a subsequent session, one can then:
    pdf = poppa.PoppaDataFrame(name='some_name')
    pdf.read_table()

## TODO

- Clean up code, make interface more like Pandas (in-progress)
- Add ability to apply arbitrary SQL expressions to data without pulling it into Python (in-progress)
- Add unit tests
- Better error handling
- Add ability to join multiple PoppaDataFrames using SQL joins
- Use SQLAlchemy to build queries everywhere (for features that SA doesn't support, still build most of the query with SA and inject additional SQL into the str)
- Python 2 support
- Add transparent int indexing/slicing support - i.e. pdf[10:20] should just work and return a pandas DF - this will allow PoPPa to be transparently used in place of a pandas DF in some places
- Add loop support (for i in pdf and for i, row in pdf.iterrows())
