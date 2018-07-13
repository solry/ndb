# ndb
Easy-to-use wrapper to common **Postgres** database operations.

1) Clone this repo:

        git clone https://github.com/solry/ndb.git

2) Import **ndb** and get start:

        
        import ndb
        db = ndb.DataBase(host='10.0.5.1', database='test_db',
                          user='test', password='12345', 
                          logging_level='debug')
        
3) To be continued