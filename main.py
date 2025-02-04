import mysql.connector
from mysql.connector import Error
from datetime import datetime
import time
import csv
import inspect
import os


today = datetime.now().strftime("%d-%m-%Y")
log_file_name = f'{today}-log.txt'

def get_date():
    now = datetime.now()
    return now.strftime("%m/%d/%Y %H:%M:%S")


def get_line_number():
    return inspect.currentframe().f_back.f_lineno


def write_log_file(text, line):
    with open(log_file_name, "a", encoding='utf-8') as file:
        file.write(f"{text} (Line: {line}, {get_date()})\n")


def get_unit_type_from_id(unit_id):
    for key, value in sale_units.items():
        if value["easy_trade_id"] == unit_id:
            return value


def write_to_csv(data, filename="output.csv"):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(data)


def create_query_arg(units_config):
    et_units_id = tuple(value["easy_trade_id"] for value in units_config.values() if value["easy_trade_id"])
    if len(et_units_id) == 1:
        return f"AND G.gd_unit = {et_units_id[0]}"
    else:
        return f"AND G.gd_unit IN {et_units_id}"


with open("config.txt", encoding='utf-8') as config_file:
    config = eval(config_file.read())

price_type = config["price_type"]
host = config["host"]
database = config["database"]
user = config["user"]
password = config["password"]
divider = config["divider"]
check_time = config["check_time"]
only_selected_group = config["only_selected_group"]
sale_units = config['sale_units']


units_arg = create_query_arg(sale_units)

query_fetch_items_with_group = f"""
    SELECT  
        G.gd_code, 
        G.gd_name, 
        G.gd_unit,  
        P.prc_value
    FROM easytrade_db.dir_goods G
        LEFT JOIN easytrade_db.dir_prices P ON G.gd_id = P.prc_good
        LEFT JOIN easytrade_db.dir_groups GR ON G.gd_group = GR.grp_id
    WHERE 
        G.gd_deleted_mark = 0 
        AND G.gd_deleted = 0 
        AND P.prc_type = %s
        AND P.prc_value > 0
        AND GR.grp_name LIKE '%#456%'
        {units_arg}
    ORDER BY G.gd_code
"""

query_fetch_items = f"""
    SELECT  
        G.gd_code, 
        G.gd_name, 
        G.gd_unit,  
        P.prc_value
    FROM easytrade_db.dir_goods G
        LEFT JOIN easytrade_db.dir_prices P ON G.gd_id = P.prc_good
    WHERE 
        G.gd_deleted_mark = 0 
        AND G.gd_deleted = 0 
        AND P.prc_type = %s
        AND P.prc_value > 0
        {units_arg}
    ORDER BY G.gd_code
"""


if not os.path.exists(log_file_name):
    with open(log_file_name, 'w', encoding='utf-8') as file:
        file.write(f"File created at {get_date()}\n")


class UpdateData:
    def __init__(self, price_type_id=price_type):
        self.mysql_conn = None
        self.mdb_conn = None
        self.mdb_cursor = None
        self.last_changes = 0
        self.price_type_id = price_type_id
        self.is_mysql_connected = False
        self.connect_mysql()

    def connect_mysql(self):
        try:
            self.mysql_conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
            )
        except Error as e:
            print(f"Can't connect to the MySQL. {e}. Line: {get_line_number()}")
            write_log_file(f"Can't connect to the MySQL. {e} {get_date()}", get_line_number())
            self.is_mysql_connected = False
            return False
        else:
            self.is_mysql_connected = True
            return True

    def check_mysql_changes(self):
        try:
            mysql_cursor = self.mysql_conn.cursor()
            mysql_cursor.execute("RESET QUERY CACHE")
            query_check_item = """
            SELECT gd_last_update FROM easytrade_db.dir_goods
            ORDER BY gd_last_update DESC
            LIMIT 1
            """
            mysql_cursor.execute(query_check_item)
            last_changed_item = mysql_cursor.fetchone()

            query_check_price = """
            SELECT prc_last_update FROM easytrade_db.dir_prices
            ORDER BY prc_last_update DESC
            LIMIT 1
            """

            mysql_cursor.execute(query_check_price)
            last_changed_price = mysql_cursor.fetchone()

            last_operation = last_changed_price if last_changed_price > last_changed_item else last_changed_item

            timestamp_last_operation = last_operation[0].timestamp()

            if self.last_changes < timestamp_last_operation:
                self.last_changes = timestamp_last_operation
                mysql_cursor.close()
                return True
            else:
                mysql_cursor.close()
                return False
        except Error as e:
            print(f"Line: {get_line_number()}, Can't connect to the MySQL. {e} {get_date()}")
            write_log_file(f"Can't connect to the MySQL. {e}", get_line_number())
            self.connect_mysql()

    def update_items(self):
        try:
            mysql_cursor = self.mysql_conn.cursor()
            mysql_cursor.execute("RESET QUERY CACHE")

            if only_selected_group:
                mysql_cursor.execute(query_fetch_items_with_group, (self.price_type_id,))
                items = mysql_cursor.fetchall()
            else:
                mysql_cursor.execute(query_fetch_items, (self.price_type_id,))
                items = mysql_cursor.fetchall()

        except Error as e:
            print(f"Line: {get_line_number()}, Can't connect to the MySQL. {e} {get_date()}")
            write_log_file(f"Can't connect to the MySQL. {e}", get_line_number())
            self.connect_mysql()

        else:
            items_list = []
            for item in items:
                unit_info_dict = get_unit_type_from_id(item[2])
                if unit_info_dict:
                    unit_id = unit_info_dict["lp_scale_id"]
                    unit_prefix = unit_info_dict["prefix"]
                    unit_label_format = unit_info_dict["label_format"]
                    unit_sale_type = unit_info_dict["sale_type"]
                    unit_barcode_type = unit_info_dict["barcode_type"]
                    item_list = [
                        item[0],
                        item[1],
                        f'{unit_prefix}{item[0]}',
                        unit_barcode_type,
                        unit_label_format,
                        item[3] / divider,
                        unit_id,
                        0,
                        0,
                        0,
                        0,
                        0,
                        unit_sale_type,
                        0,
                        0,
                        0,
                        0
                    ]
                    items_list.append(item_list)

            write_to_csv(items_list, filename='products.csv')


update_data = UpdateData()


while True:
    if update_data.is_mysql_connected:
        is_changed = update_data.check_mysql_changes()

        if is_changed:
            update_data.update_items()
            print("changed")
            write_log_file("changed", get_line_number())
    else:
        update_data.connect_mysql()

    time.sleep(check_time)

