#!/usr/bin/python3
# -*- coding: utf-8 -*-

import MySQLdb
import json

#DATABASE CONFIGURATION
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWD = "root"
DB_DATABASE = "tcgplace"

#LOCAL WEB SERVICE CONFIGURATION
WS_LOCALHOST = "0.0.0.0"
WS_PORT = 8088

#RABBITMQ CONFIGURATION
RB_HOST = "localhost"
RB_PORT = 5672
RB_USER = "guest"
RB_PASSWD = "guest"

#LOCAL DATABASE FILE
LOCAL_DB_FILE = "/tmp/cards_db.txt"

#dictionary with sql commands
SQL_COMMANDS = {'show_all_tables':"SHOW TABLES",
               'show_columns_names':"SHOW COLUMNS FROM ",
               'show_columns_names_table_magiccard':"SHOW COLUMNS FROM magiccard",
               'show_columns_names_table_magicexpansion':"SHOW COLUMNS FROM magicexpansion",
               'select_all_columns_table_magiccard':"SELECT * FROM magiccard",
               'select_id_expasion_table_magiccard':"SELECT * FROM magiccard WHERE ExpansionId=",
               'select_name_expasion_table_magicexpansion':"SELECT Name FROM magicexpansion WHERE ExpansionId="}

# Conecting to database in DB_HOST:DB_PORT as DB_USER:DB_PASSWD
def make_connection():
    db = MySQLdb.connect(host=DB_HOST,
                         port=DB_PORT,
                         user=DB_USER,
                         passwd=DB_PASSWD,
                         db=DB_DATABASE)

    return db

# Return list of json cards
def make_json_card(columns_name,cards):
    list_json_cards = []
    for card in cards:
        dict_card = dict(zip(columns_name,list(card)))
        list_json_cards.append(json.dumps(dict_card))

    return list_json_cards

#Get columns_name
def get_columns_name(tablename):
    db = make_connection()
    cursor = db.cursor()
    query = SQL_COMMANDS['show_columns_names'] + tablename
    cursor.execute(query)
    columns_name = []

    for name in cursor:
        columns_name.append(name[0])

    return columns_name

def get_info_from_database(query='show_all_tables'):
    db = make_connection()
    cursor = db.cursor()
    cursor.execute(SQL_COMMANDS[query])

    for row in cursor.fetchall():
        print(row)

    db.close()
