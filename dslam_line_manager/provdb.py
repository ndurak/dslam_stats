#!/usr/bin/python

import cx_Oracle
import sys

def provconnect():
  try:
    dsn = cx_Oracle.makedsn('oradb-ha.iskon.hr', 1521, 'ha')
    conn = cx_Oracle.connect('prov_ro', 'provNG2', dsn)
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
