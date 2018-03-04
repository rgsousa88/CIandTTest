#!/usr/bin/python3
# -*- coding: utf-8 -*-

import MySQLdb
import flask
import requests
import pika
import pandas as pd
import json
import celery

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
RB_HOST = "127.0.0.1"
RB_PORT = 15672
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


columns_name = get_columns_name("magiccard")

credentials = pika.PlainCredentials(RB_USER,RB_PASSWD)
parameters = pika.ConnectionParameters(RB_HOST,
                                       RB_PORT,
                                       '/',
                                       credentials)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='cards')


app = flask.Flask("magicDeckServer")


@app.route('/movecards/<expansion_id>',methods=['POST'])
def movecards(expansion_id):
    db = make_connection()
    cursor = db.cursor()

    sql_query = SQL_COMMANDS['select_id_expasion_table_magiccard'] + expansion_id
    cursor.execute(sql_query)
    cards = list(cursor)

    if not cards:
        ans = "ExpansionId " + expansion_id + " not found."
        db.close()
        return flask.Response(response=ans,status=404)

    json_cards = make_json_card(columns_name,cards)

    #publish json_cards to exchange

    sql_query = SQL_COMMANDS['select_name_expasion_table_magicexpansion'] + expansion_id
    cursor.execute(sql_query)
    expansionName = cursor.fetchone()[0]

    ans = {"Expansion Name": expansionName,
            "Posted Cards": len(cards)}

    db.close()
    #connection.close()
    return flask.Response(response=json.dumps(ans),status=200)
    #pass
#
@app.route('/moveall',methods=['GET'])
def moveall():
    """Criar um serviço assíncrono que deve retornar 202 assim que for chamado
       e em background ler a tabela de expansion e acionar o serviço do item 1.
    """


    pass

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
