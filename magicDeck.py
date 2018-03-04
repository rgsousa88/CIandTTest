#!/usr/bin/python3
# -*- coding: utf-8 -*-


import flask
import requests
import pika
import pandas as pd
import celery
import myutils as mu
import json

app = flask.Flask("magicDeckServer")

@app.route('/movecards/<expansion_id>',methods=['POST'])
def movecards(expansion_id):
    db = mu.make_connection()
    cursor = db.cursor()

    sql_query = mu.SQL_COMMANDS['select_id_expasion_table_magiccard'] + expansion_id
    cursor.execute(sql_query)
    cards = list(cursor)

    if not cards:
        ans = "ExpansionId " + expansion_id + " not found."
        db.close()
        return flask.Response(response=ans,status=404)

    columns_name = mu.get_columns_name("magiccard")
    json_cards = mu.make_json_card(columns_name,cards)

    #publish json_cards to exchange
    credentials = pika.PlainCredentials(mu.RB_USER,mu.RB_PASSWD)
    parameters = pika.ConnectionParameters(mu.RB_HOST,
                                           mu.RB_PORT,
                                           '/',
                                           credentials)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='cards')


    channel.exchange_declare(exchange='cards',
                            exchange_type='direct')

    for card in json_cards:
        channel.basic_publish(exchange='cards',
                              routing_key='moving_cards',
                              body=card)


    sql_query = mu.SQL_COMMANDS['select_name_expasion_table_magicexpansion'] + expansion_id
    cursor.execute(sql_query)
    expansionName = cursor.fetchone()[0]

    ans = {"Expansion Name": expansionName,
            "Posted Cards": len(cards)}

    db.close()
    connection.close()
    return flask.Response(response=json.dumps(ans),status=200)

#
@app.route('/moveall',methods=['GET'])
def moveall():
    """Criar um serviço assíncrono que deve retornar 202 assim que for chamado
       e em background ler a tabela de expansion e acionar o serviço do item 1.
    """


    pass

#
@app.route('/card/<card_id>',methods=['GET'])
def get_card_info():
    """Criar um serviço para obter as informações de um card
       gravado no arquivo "/tmp/cards_db.txt" pelo seu id
    """

    #pd.read_csv('')

    pass

#get_info_from_database('show_columns_names_table_magicexpansion')

#db.close()

if __name__ == '__main__':
    app.run(host=mu.WS_LOCALHOST, port=mu.WS_PORT)
