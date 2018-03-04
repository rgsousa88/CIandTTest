#!/usr/bin/python3
# -*- coding: utf-8 -*-

import MySQLdb
import flask
import requests
import pika

#enviroment and configuration variables
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWD = "root"
DB_DATABASE = "tcgplace"
WS_LOCALHOST = "0.0.0.0"
WS_PORT = "8088"
LOCAL_DB_FILE = "/tmp/cards_db.txt"

#dictionary with sql queries
QUERYS = {'show_all_tables':"SHOW TABLES",
          'show_columns_names_table_magicard':"SHOW COLUMNS FROM magiccard",
          'show_columns_names_table_magicexpansion':"SHOW COLUMNS FROM magicexpansion",
          'select_all_columns_table_magiccard':"SELECT * FROM magiccard",
          'select_id_expasion_table_magiccard':"SELECT ExpansionId FROM magiccard"}

# Conecting to database in DB_HOST:DB_PORT as DB_USER:DB_PASSWD
db = MySQLdb.connect(host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    passwd=DB_PASSWD,
                    db=DB_DATABASE)

def get_info_from_database(query='show_all_tables'):
    cursor = db.cursor()
    cursor.execute(QUERYS[query])

    for row in cursor.fetchall():
        print(row)

# app = flask.Flask("magicDeckServer")
#
# @app.route('/movecards/:expansion_id',methods=['POST'])
# def movecards():
#     pass
#
# @app.route('/moveall',methods=['GET'])
# def moveall():
#     pass
#
# @app.route('/card/:card_id',methods=['GET'])
# def get_card_info():
#     pass

get_info_from_database('show_columns_names_table_magicexpansion')

db.close()

# if __name__ == '__main__':
#     app.run(host=WS_LOCALHOST, port=WS_PORT)
