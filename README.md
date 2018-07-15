# ndb
Easy-to-use wrapper to common **Postgres** database operations.

1) Clone this repo:

        git clone https://github.com/solry/ndb.git

2) Import **ndb** and get start:
        
        import ndb
        db = ndb.DataBase(host='10.0.5.1', database='test_db',
                          user='test', password='12345', logging_level='debug')
        
    note: set logging_level to adjust or turn off logging messages.  
    Choose from [ None, 'debug', 'info', 'warning', 'error' ]  
    This param set logging severity for all log messages of this module
        
3) Use **insert()** to add new entry to db:

        db.insert('test_table', dict(name='test_name',address='test_address'))
        Out[5]: 1
        
   Result: 1 - number of rows affected. In successful INSERT statement it will be always **1**
   
   note: if you like to use RETURNING statement in the INSERT - just pass to the method **returning** key:
        
        db.insert('test_table', dict(name='test_name',address='test_address'), returing='id')
        out[6]: 10
    
   Result: 1 - attribute **id** of inserted row.
    
   note: query above is translated to next SQL query:  
        
        INSERT INTO test_table (name,address)   
        VALUES ($insert_string$test_name$insert_string$,$insert_string$test_address$insert_string$)  
        RETURNING address  
        
4) Use regular **query()** to get list of tuples and **dquery()** to get list of dictionary:
        
        db.dquery('SELECT * FROM test_table')
        Out[10]: [{'id':10, 'name': 'test_name', 'address': 'test_address'}]
        db.query('SELECT name, id FROM test_table')
        Out[12]: [('test_name', 10)]

5) Use **update()** method to easily update rows in dict-style mapping:

        db.update('test_table', dict(name='test_RENAME',address='test_address'), where="name = 'test_name'")
        out[6]: 1
        
   Result: 1 - number of rows affected
   