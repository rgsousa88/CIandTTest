#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pika
import pandas as pd
import json
import celery
import re
import myutils as mu
import lock as lk


credentials = pika.PlainCredentials(mu.RB_USER,mu.RB_PASSWD)
parameters = pika.ConnectionParameters(mu.RB_HOST,
                                       mu.RB_PORT,
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
    card = json.loads(body.decode('utf-8'))
    print("Receiving card...")

    try:
        lock = lk.Lock("/tmp/lock_name.tmp")
        lock.acquire()
        with open(mu.LOCAL_DB_FILE,'a') as file:
            values = list(card.values())
            file.write(";".join(str(value) for value in values))
            file.write('\n')
    finally:
        lock.release()


channel.basic_consume(callback,
                      queue='cards',
                      no_ack=True)

channel.start_consuming()
