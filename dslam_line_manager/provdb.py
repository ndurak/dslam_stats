#!/usr/bin/python

import cx_Oracle
import sys

def provconnect():
  try:
    dsn = cx_Oracle.makedsn('oradb.net', 1521, 'oradb')
    conn = cx_Oracle.connect('oradb', 'pw123', dsn)
    cursor = conn.cursor()
    return conn, cursor
  except Exception, e:
    print "Exception: " +str(e)
    sys.exit()
    

def provclose(conn, cursor):
  try:
    cursor.close()
    conn.close()
  except Exception, e:
    print "Exception: " + str(e)
    sys.exit()
