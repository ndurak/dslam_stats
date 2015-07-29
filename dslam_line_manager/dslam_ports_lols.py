#!/usr/bin/python

import datetime
import MySQLdb
from decimal import *
import redis


getcontext().prec=1
getcontext().rounding=ROUND_FLOOR

conn = MySQLdb.connect (host = "mysqldb.net",
                        user = "user",
                        passwd = "pw123",
                        db = "dslam")
conn.autocommit(True)
cursor = conn.cursor()

red = redis.StrictRedis(host="redishost.myorg.net",port=6379,db=2)

starttime = (datetime.datetime.now() - datetime.timedelta(hours=6)).replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
endtime = datetime.datetime.now().replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")

#dictionary of type dslam_id: ip
dslams = {}
query = """select dslam_id, ip from dslami where ip != ''"""
cursor.execute(query)
rows = cursor.fetchall()
for row in rows:
    dslams[row[0]] = row[1]

#dictionary of type ifIndex: port_id
ports = {}
query = """select ifIndex, id from ports"""
cursor.execute(query)
rows = cursor.fetchall()
for row in rows:
    ports[row[0]] = row[1]


vdslBoardTypes = ['H80BVDPM','H80AVDPD']
adslBoardTypes = ['H561ADBF','H565ADBF','H565ADBF2','H565ADBF3','H808ADPM','H802ADQD']


def getPortId(dslam_id,frame,slot,port):
    board = "%s/%s"%(frame,slot)
    key = "boards:%s:%s"%(dslams[dslam_id],board)
    boardtype = red.get(key)
    ifIndex = None
    if boardtype in vdslBoardTypes:
        ifIndex = 4160749568 + 8192*slot + 64*port
    elif boardtype in adslBoardTypes:
        ifIndex = 201326592 + 8192*slot + 64*port
    try:
        return ports[ifIndex]
    except KeyError:
        return None



def extract_lines_with_lols():
    events = {}
    # events from T dslams
    query = """select SUM(case when event = 1 then 1 else 0 end) as activated, 
            SUM(case when event=10 then 1 else 0 end) as activating, 
            SUM(case when event=11 then 1 else 0 end) as deactivated,dslam_id,
            frame,slot,port from dslam_events where time between '%s' and '%s'
            and retrain_reason_id not in (45,46,47,48)
            group by dslam_id,frame,slot,port having activating > 1"""%(starttime,endtime)
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        dslam_id = row[3]
        frame = row[4]
        slot = row[5]
        port = row[6]
        port_id = getPortId(dslam_id,frame,slot,port)
        dslamportkey = (dslam_id<<14) + port_id
        events[dslamportkey] = {}
        events[dslamportkey]['lols'] = row[1] - row[2]

    query = """select l.dslam_id, l.port_id, count(*) from dslam_lols l left join dslami d 
            on d.dslam_id=l.dslam_id left join portovi p on p.port_id=l.port_id where 
            `update` between '%s' and '%s' and time < 55 and dslam_type_id=1 group by 
            d.dslam_id, p.port_id having count(*) > 1"""%(starttime,endtime)
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        dslam_id = row[0]
        port_id = row[1]
        dslamportkey = (dslam_id<<14) + port_id
        events[dslamportkey] = {}
        events[dslamportkey]['lols'] = row[2]

    return events


if __name__ == '__main__':
    events = extract_lines_with_lols()
    insert_data = []
    for dslamportkey, event in events.iteritems():
        dslam_id = dslamportkey >> 14
        port_id = dslamportkey % (dslam_id<<14)
        lols = event['lols']
        insert_data.append((dslam_id, port_id, lols))

    query = """insert into dslam_ports_lolscount (dslam_id,port_id,lols) values (%s,%s,%s)"""
    cursor.executemany(query,insert_data)

