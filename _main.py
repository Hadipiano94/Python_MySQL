import mysql.connector
import csv
import datetime as dt


class SqlExecutor:
    def __init__(self, mydb, mycurser):
        self.db = mydb
        self.curser = mycurser

        self.execution_time = 0
        self.rows_effected = 0
        self.exception = ""
        self.success = False
        self.results = []

    def execute_sql_code(self, sql_command, sql_code: str, vals=None):
        the_exp = None
        val = None
        val_valid = False
        vals_valid = False
        if vals is not None:
            if len(vals) == 1:
                val = vals[0]
                val_valid = True
            elif len(vals) > 1:
                vals_valid = True

        # command execution and timing
        if sql_command in ["INSERT", "UPDATE", "DELETE"] and vals_valid:
            sql_st = dt.datetime.now().timestamp()
            try:
                self.curser.executemany(sql_code, vals)
            except Exception as exp:
                the_exp = exp
            sql_fin = dt.datetime.now().timestamp()
        elif sql_command in ["INSERT", "UPDATE", "DELETE"] and val_valid:
            sql_st = dt.datetime.now().timestamp()
            try:
                self.curser.execute(sql_code, val)
            except Exception as exp:
                the_exp = exp
            sql_fin = dt.datetime.now().timestamp()
        else:
            sql_st = dt.datetime.now().timestamp()
            try:
                self.curser.execute(sql_code)
            except Exception as exp:
                the_exp = exp
            sql_fin = dt.datetime.now().timestamp()

        # commit if needed
        if sql_command in ["CREATE", "INSERT", "DELETE", "DROP", "ALTER", "UPDATE"]:
            try:
                self.db.commit()
            except Exception as exp:
                the_exp = exp

        # data fetching
        if sql_command in ["SHOW", "SELECT", "DESCRIBE"]:
            try:
                self.results = self.curser.fetchall()
            except Exception as exp:
                the_exp = exp

        # rows count
        if sql_command in ["INSERT", "CREATE", "UPDATE", "SELECT", "DELETE"]:
            try:
                self.rows_effected = self.curser.rowcount
            except Exception as exp:
                the_exp = exp

        # success check
        if self.curser.rowcount >= 0:
            self.success = True
        else:
            self.success = False

        self.exception = the_exp

        self.execution_time = round(sql_fin - sql_st, ndigits=3)


def make_database(db_name: str,
                  host: str,
                  user: str,
                  password: str,
                  ):
    try:
        my_db = mysql.connector.connect(host=host,
                                        user=user,
                                        password=password,
                                        )
    except Exception as exp:
        print(f"- something went wrong connecting sql. the sql error:\n    {exp}")
        raise SystemExit

    my_curser = my_db.cursor()
    try:
        my_curser.execute(f"CREATE DATABASE {db_name};")
        print(f"- database '{db_name}' created successfully")
    except Exception as exp:
        print(f"- something went wrong making the database. the sql error:\n    {exp}")
        raise SystemExit


def run_sql_executor(db_name, sql_code=None, values=None):
    if len(db_name) == 0:
        print("- please provide a database name and run the code again")
        raise SystemExit
    else:

        # checking everything
        db_exists = False
        sql_exists = False

        try:
            with open("db_info.txt", "r") as db_info_file:
                db_lines_all = db_info_file.readlines()
                db_lines = []
                for line in db_lines_all:
                    if line.count("host") or line.count("user") or line.count("pass"):
                        db_lines.append(line)
                db_lines = db_lines[:3]
                db_exists = True

        except FileNotFoundError:
            with open("db_info.txt", "w") as db_info_file:
                db_info_file.write("host=\nuser=\npassword=\n\n\nAttention! keep the following format at the first lines of the file \nin order for it to work correctly:\n\nhost=your_host\nuser=your_user\npassword=your_password\n")
            db_lines = []
            print("- 'db_info.txt' created. please write your db information inside this file in the required format and run the code again.")

        if sql_code is None:
            try:
                with open("sql_code.sql", "r") as sql_file:
                    my_sql_code = sql_file.read()
                    sql_exists = True

            except FileNotFoundError:
                with open("sql_code.sql", "w") as _:
                    pass
                my_sql_code = ""
                print("- 'sql_code.sql' created.please write your sql code inside this file and run the code again.")
        else:
            my_sql_code = sql_code
            sql_exists = True

        # files validating
        if not (db_exists and sql_exists):
            raise SystemExit
        else:
            pass

        if len(db_lines) == 0:
            print("- please write your db information inside db_info.txt file in the following format and run the code again:\n\nhost=\nuser=\npassword=\ndatabase=\n")
            raise SystemExit

        my_host = db_lines[0].strip().split("=")[1]
        my_user = db_lines[1].strip().split("=")[1]
        my_password = db_lines[2].strip().split("=")[1]
        my_database = db_name

        if db_exists:
            try:
                my_db = mysql.connector.connect(
                    host=my_host,
                    user=my_user,
                    password=my_password,
                    database=my_database,
                )
                my_curser = my_db.cursor()
            except Exception as _exp:
                print(f"- there is something wrong with your database information. the sql return error is:\n    {_exp}")
                raise SystemExit
        else:
            print("- please fill in the database information first and run the code again.")
            raise SystemExit

        if len(my_sql_code.strip()) == 0:
            print("- please write your sql code in the sql_code.sql file and run the code again.")
            raise SystemExit

        else:
            my_sql_command = my_sql_code.split()[0].upper()
            my_vals = None
            # INSERT, UPDATE values
            if my_sql_command in ["INSERT", "UPDATE", "DELETE"]:
                if my_sql_code.count("%s") > 0:
                    if values is None:
                        try:
                            with open("values.csv", "r") as values_file:
                                my_vals = list(csv.reader(values_file))
                        except FileNotFoundError:
                            with open("values.csv", "w") as _:
                                print("- 'values.csv' created. please write or copy your values inside this file in a csv format and run the code again.")
                                raise SystemExit

                        if len(my_vals) == 0:
                            print("- please provide the values in the values.csv file and run the code again.")
                            raise SystemExit
                    else:
                        my_vals = values

            my_sql_executor = SqlExecutor(mydb=my_db, mycurser=my_curser)
            my_sql_executor.execute_sql_code(sql_command=my_sql_command,
                                             sql_code=my_sql_code,
                                             vals=my_vals,
                                             )
            # making the report
            report_str = f"\n- no problem occurred before running sql\n\nSQL Report from '{db_name}' database:\n- {my_sql_executor.success}\n"

            if my_sql_executor.exception is not None:
                report_str += f"- {my_sql_executor.exception}\n"
            else:
                pass

            if len(my_sql_executor.results) != 0:
                items_str = ""
                for item in my_sql_executor.results:
                    items_str += f"     {str(item)}\n"
                report_str += f"- items:\n{items_str}"
            else:
                pass

            if my_sql_executor.rows_effected > 0:
                report_str += f"- {my_sql_executor.rows_effected} rows effected.\n"
            else:
                pass

            report_str += f"- execution time:{my_sql_executor.execution_time}\n"

            print(report_str)
            if len(my_sql_executor.results) != 0:
                return my_sql_executor.results
            else:
                return None
