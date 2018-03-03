#!/usr/bin/python3
# -*- coding: utf-8 -*-

import MySQLdb

DB_HOST = "127.0.0.1"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWD = "root"
DB_DATABASE = "tcgplace"

db = MySQLdb.connect(host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    passwd=DB_PASSWD,
                    db=DB_DATABASE)

cursor = db.cursor()

cursor.execute("SHOW TABLES")

for row in cursor.fetchall():
    print(row)

db.close()
