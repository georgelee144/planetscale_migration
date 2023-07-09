import mysql.connector
import glob
import os
from dotenv import load_dotenv
import logging

load_dotenv()


def read_and_create_from_sql(sql_file):
    with mysql.connector.connection.MySQLConnection(
        host="127.0.0.1",
        user=os.getenv("USERNAME"),
        password=os.getenv("PASSWORD"),
        database=os.getenv("DATABASE"),
    ) as connection:
        with connection.cursor() as cursor:
            read_query = open(sql_file).read()
            cursor.execute(read_query)
            connection.commit()


if __name__ == "__main__":
    planet_scale_dump_dir = "/home/george/pscale_dump_dividend_record_main"

    all_schemas = glob.glob(f"{planet_scale_dump_dir}/*schema.sql")
    for file in all_schemas:
        try:
            read_and_create_from_sql(file)
        except Exception as ex:
            logging.error(f"Failed to create from {file}: \n{ex}")

    all_tables = glob.glob(f"{planet_scale_dump_dir}/*00001.sql")
    for file in all_tables:
        try:
            read_and_create_from_sql(file)
        except Exception as ex:
            logging.error(f"Failed to create from {file}: \n{ex}")
