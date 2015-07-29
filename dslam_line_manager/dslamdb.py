#!/usr/bin/python

import MySQLdb
import sys

def dslamconnect():
  try:
    conn = MySQLdb.connect(host = "rpmiptvdb.zg.iskon.hr",
                          user = "dslam",
                          passwd = "stats",
                          db = "dslam")
    cursor = conn.cursor()
    return conn, cursor
  except Exception, e:
    print "Exception: " + str(e)
    sys.exit()


def dslamclose(conn, cursor):
  try:
    cursor.close()
    conn.close()
    return True
  except Exception, e:
    print e
    return False
