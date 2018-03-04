#!/usr/bin/python3
# -*- coding: utf-8 -*-

import MySQLdb
import flask
import requests
import pika
import pandas as pd
import json

#enviroment and configuration variables
DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWD = "root"
DB_DATABASE = "tcgplace"
WS_LOCALHOST = "0.0.0.0"
WS_PORT = 8088
LOCAL_DB_FILE = "/tmp/cards_db.txt"

#dictionary with sql commands
SQL_COMMANDS = {'show_all_tables':"SHOW TABLES",
               'show_columns_names_table_magicard':"SHOW COLUMNS FROM magiccard",
               'show_columns_names_table_magicexpansion':"SHOW COLUMNS FROM magicexpansion",
               'select_all_columns_table_magiccard':"SELECT * FROM magiccard",
               'select_id_expasion_table_magiccard':"SELECT * FROM magiccard"}

# Conecting to database in DB_HOST:DB_PORT as DB_USER:DB_PASSWD

def make_connection():
    db = MySQLdb.connect(host=DB_HOST,
                         port=DB_PORT,
                         user=DB_USER,
                         passwd=DB_PASSWD,
                         db=DB_DATABASE)

    return db

def get_info_from_database(query='show_all_tables'):
    db = make_connection()
    cursor = db.cursor()
    cursor.execute(SQL_COMMANDS[query])

    for row in cursor.fetchall():
        print(row)

    db.close()

app = flask.Flask("magicDeckServer")

@app.route('/movecards/<expansion_id>',methods=['POST'])
def movecards(expansion_id):
    db = make_connection()
    cursor = db.cursor()

    ans = {"idExpansion":expansion_id}

    sql_query = SQL_COMMANDS['select_id_expasion_table_magiccard'] + " WHERE ExpansionId=" + expansion_id
    cursor.execute(sql_query)

    for row in cursor.fetchall():
        print(row)
        #print(flask.jsonify(row))
        #print(json.loads(row))

    db.close()

    return flask.jsonify(ans)
    #pass
#
#@app.route('/moveall',methods=['GET'])
#def moveall():
# Criar um serviço assíncrono que deve retornar 202 assim que for chamado
# e em background ler a tabela de expansion e acionar o serviço do item 1.
#     pass
#
# @app.route('/card/:card_id',methods=['GET'])
# def get_card_info():
# Criar um serviço para obter as informações de um card
# gravado no arquivo "/tmp/cards_db.txt" pelo seu id
#
#     pass

#get_info_from_database('show_columns_names_table_magicexpansion')

#db.close()

if __name__ == '__main__':
    app.run(host=WS_LOCALHOST, port=WS_PORT)
