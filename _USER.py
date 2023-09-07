from _main import run_sql_executor, make_database


"""in case you want to create a new database, run this function. give every 
attribute as string."""

# make_database(db_name="hadi_test_3",
#               host="localhost",
#               user="root",
#               password="Hadi@1994")


"""if you want to write sql here save it in my_sql_code variable, otherwise 
sql code would be read from sql_code.sql. also you can save your %s values 
as a list in vals variable if you want, if you leave it as None, the values 
would be read from values.csv. note that the function also return the results 
if sql command is either SHOW, SELECT or DESCRIBE."""


my_sql_code = None
vals = None

run_sql_executor(db_name="hadi_test_1",
                 sql_code=my_sql_code,
                 values=vals)
