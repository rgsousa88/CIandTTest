#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pika
import pandas as pd
import json
import celery

RB_HOST = "localhost"
RB_PORT = 5672
RB_USER = "guest"
RB_PASSWD = "guest"

LOCAL_DB_FILE = "/tmp/cards_db.txt"

credentials = pika.PlainCredentials(RB_USER,RB_PASSWD)
parameters = pika.ConnectionParameters(RB_HOST,
                                       RB_PORT,
                                       '/',
                                       credentials)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()


channel.exchange_declare(exchange='cards',
                         exchange_type='direct')

channel.queue_declare(queue='cards')

channel.queue_bind(exchange='cards',
                   queue='cards',
                   routing_key='moving_cards')


print(' [*] Waiting for cards. To exit press CTRL+C')

def callback(ch, method, properties, body):
    #card = json.loads(body.decode('utf-8'))
    print("Receiving %s"%(json.loads(body)))

channel.basic_consume(callback,
                      queue='cards',
                      no_ack=True)

channel.start_consuming()
