import snowflake.connector
import psycopg2
import yaml
import re

SF_USER = ''
SF_PASSWORD = ''
SF_ACCOUNT = ''
SF_WAREHOUSE = ''
SF_DATABASE = ''
SF_SCHEMA = ''

PG_USER = ""
PG_PASSWORD = ""
PG_HOST = ""
PG_PORT = "5432"
PG_DATABASE = ""


class ETL_Session:

    def __init__(self):
        self.statement = None
        self.sf_sql = None

        self.sf_con = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            schema=SF_SCHEMA
        )
        self.pg_con = psycopg2.connect(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE
        )

        # Make sure cursors are working are set
        self.sf_cur = self.sf_con.cursor()
        self.sf_cur.execute('select 1;')

        self.pg_cur = self.pg_con.cursor()
        self.pg_cur.execute('select 1;')


    def create_script(self):
        '''
        Use yml file to create sql insert script. Need destination table and all columns. Optional contraint_key
        :return:
        '''
        with open('./config/column.yml') as file:
            y_config = yaml.load(file, Loader=yaml.BaseLoader)

        insert_table = y_config['table']
        column_list = y_config['columns']
        constraint_key = y_config.get('constraint_key', None)

        # Create the list of %s for sql placeholder script
        num_columns = len(column_list)
        per_s = '(' + '%s,' * (num_columns - 1) + '%s)'

        # Remove quotations from the string when creating tuple string and table string
        insert_table_str = re.sub("\'", '', str(insert_table[0]))
        column_list_tuple_str = re.sub("\'", '', str(tuple(column_list)))

        # Insert section of script
        part1 = "INSERT INTO {table}{column_tuple} VALUES {per_s}".format(table=insert_table_str,
                                                                          column_tuple=column_list_tuple_str,
                                                                          per_s=per_s)
        # If constraint_key exists, set update script on constraint
        if constraint_key:
            update_columns = [c + " = excluded." + c for c in column_list]
            update_columns_str = re.sub('\'|\[|\]', '', str(update_columns))
            part2 = ' ON CONFLICT ON CONSTRAINT {constraint_key} DO UPDATE SET '.format(
                constraint_key=constraint_key[0]) + update_columns_str
        else:
            part2 = ''

        # full statement
        self.statement = part1 + part2
        print(self.statement)

    def run(self):
        '''
        Use psycopg2 execute many to run inserts, do this in batch so we don't kill the database
        :return None:
        '''
        # Read in the Snowflake SQL
        with open('./config/snowflake.sql') as file:
            self.sf_sql = file.read()
        self.sf_cur.execute(self.sf_sql)

        while True:
            records = self.sf_cur.fetchmany(size=10)
            if not records:
                break
            self.pg_cur.executemany(self.statement, records)
            self.pg_con.commit()

        self.pg_con.close()
