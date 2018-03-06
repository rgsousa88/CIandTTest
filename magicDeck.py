#!/usr/bin/python3
# -*- coding: utf-8 -*-

import flask
import requests
import pika
import pandas as pd
import myutils as mu
import json
import lock as lk
from myutils import async


broker = 'amqp://guest:guest@127.0.0.1:5672'

app = flask.Flask("magicDeckServer")

@async
def background_task():
    list_expansion_ids = mu.get_info_from_database(query='select_all_id_expasion_table_magicexpansion')

    for expansion_id in list_expansion_ids:
        print("SENDING %d"%(expansion_id))
        movecards(expansion_id=str(expansion_id))

    return 0


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
        print("SENDING...")
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
    task = background_task()
    return flask.Response(response={"202 - Accepted"},status=202)


@app.route('/card/<card_id>',methods=['GET'])
def get_card_info(card_id):
    """Criar um serviço para obter as informações de um card
       gravado no arquivo "/tmp/cards_db.txt" pelo seu id
    """
    columns_name = mu.get_columns_name("magiccard")
    query_card = pd.DataFrame()

    try:
        lock = lk.Lock("/tmp/lock_name.tmp")
        lock.acquire()
        dtFrame = pd.read_csv(mu.LOCAL_DB_FILE,
                             delimiter=",,",
                             header=None,
                             names=columns_name,
                             engine='python')

        query_card = dtFrame.query('GathererId == @card_id')

    finally:
        lock.release()

    if(query_card.empty):
        ans = "GathererId " + card_id + " not found."
        status = 400
    else:
        ans = json.dumps(json.loads(query_card.to_json(orient='records')), indent=2)
        status = 200

    return flask.Response(response=ans,status=status)


if __name__ == '__main__':
    app.run(host=mu.WS_LOCALHOST, port=mu.WS_PORT)
