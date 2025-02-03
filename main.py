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


with open("config.txt", encoding='utf-8') as config_file:
    config = eval(config_file.read())

price_type = config["price_type"]
host = config["host"]
database = config["database"]
user = config["user"]
password = config["password"]
divider = config["divider"]
use_piece = config["use_piece"]
check_time = config["check_time"]
weight_prefix = config["weight_prefix"]
piece_prefix = config["piece_prefix"]
barcode_type = config["barcode_type"]
piece_label_format = config["piece_label_format"]
weight_label_format = config["weight_label_format"]
only_selected_group = config["only_selected_group"]

sale_units = config['sale_units']
sale_units_tuple = tuple(filter(None, sale_units.values()))

lp_units = config['weight_units_lp']


def get_unit_type_from_id(unit_id):
    unit_key = None
    for key, value in sale_units.items():
        if value == unit_id:
            unit_key = key
            break
    if unit_key == "piece":
        return 2, piece_prefix, piece_label_format
    elif unit_key:
        return lp_units.get(unit_key), weight_prefix, weight_label_format


def write_to_csv(data, filename="output.csv"):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(data)


class UpdateData:
    def __init__(self, price_type_id=price_type):
        self.mysql_conn = None
        self.mdb_conn = None
        self.mdb_cursor = None
        self.last_changes = 0
        self.price_type_id = price_type_id

        if not os.path.exists(log_file_name):
            with open(log_file_name, 'w', encoding='utf-8') as file:
                file.write(f"File created at {self.get_date()}\n")

    def get_date(self):
        now = datetime.now()
        return now.strftime("%m/%d/%Y %H:%M:%S")

    def connect_mysql(self):
        try:
            self.mysql_conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
            )
        except Error as e:
            write_log_file(f"Can't connect to the MySQL. {e} {self.get_date()}", get_line_number())
            return False
        else:
            return True

    def check_mysql_changes(self):
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

    def update_items(self):
        mysql_cursor = self.mysql_conn.cursor()
        mysql_cursor.execute("RESET QUERY CACHE")

        if only_selected_group:
            mysql_query = """
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
                ORDER BY G.gd_code
            """
        else:
            mysql_query = """
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
                ORDER BY G.gd_code
            """

        mysql_cursor.execute(mysql_query, (self.price_type_id, ))
        items = mysql_cursor.fetchall()
        items_list = []
        for item in items:
            unit_info_tuple = get_unit_type_from_id(item[2])
            if unit_info_tuple:
                unit_id = unit_info_tuple[0]
                unit_prefix = unit_info_tuple[1]
                unit_label_format = unit_info_tuple[2]
                item_list = [
                    item[0],
                    item[1],
                    f'{unit_prefix}{item[0]}',
                    barcode_type,
                    unit_label_format,
                    item[3] / divider,
                    unit_id,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                ]
                items_list.append(item_list)
                print(item_list)

        write_to_csv(items_list, filename='products.csv')


update_data = UpdateData()
mysql_connection = update_data.connect_mysql()


while True:
    if mysql_connection:
        is_changed = update_data.check_mysql_changes()
        # print(is_changed)
        write_log_file(f"Change status: {is_changed}", get_line_number())
        if is_changed:
            update_data.update_items()
            # print("changed")
            write_log_file("changed", get_line_number())
    else:
        mysql_connection = update_data.connect_mysql()

    time.sleep(check_time)

