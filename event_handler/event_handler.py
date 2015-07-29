#!/usr/bin/python

from __future__ import absolute_import
import MySQLdb
import logging
import redis
import json
import time
from gevent import monkey; monkey.patch_socket()
from pysnmp.entity.rfc3413.oneliner import cmdgen
from pysnmp.proto import rfc1902
import netsnmp
import binascii
import sys
import gevent
import re

sys.path.append('/opt/apps/dslam_stats2_web/DslamManager')
import Ma5600SnmpApi
import Ma5600TSnmpApi

ma5600 = Ma5600SnmpApi.Ma5600SnmpApi()
ma5600t = Ma5600TSnmpApi.Ma5600TSnmpApi()

logger = logging.getLogger('EVENTHANDLER')
logger.setLevel(logging.DEBUG)
FORMAT = "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s"
formatter = logging.Formatter(FORMAT)
fh = logging.FileHandler('/var/log/apps/dslam_stats2_web/event_handler.log')
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
cmdGen = cmdgen.CommandGenerator()
red_srvports = redis.StrictRedis(host='redishost.myorg.net', port=6379)
red_events = redis.StrictRedis(host='redishost.myorg.net', port=6379, db=1)
red_boards = redis.StrictRedis(host="redishost.myorg.net", port=6379, db=2)



conn = MySQLdb.connect(host = "mysqldb.net",
                  user = "user",
                  passwd = "pw123",
                  db = "dslam")
conn.autocommit(True)
cursor = conn.cursor()

vdsl2LineStatusLastRetrainInfo = '1.3.6.1.4.1.2011.6.115.1.1.1.1.44'
hwadsl2LineStatusLastRetrainInfo = '1.3.6.1.4.1.2011.6.138.1.1.1.1.8'
vdslBoardTypes = ['H80BVDPM', 'H80AVDPD', 'HS3BVCMM']
adslBoardTypes = ['H808ADPM', 'H802ADQD']
ma5600BoardTypes = ['H561ADBF','H565ADBF','H565ADBF2','H565ADBF3']
community = 'public'

dslams = {}
dslams_inv = {}
ports = {}
adslports = {}
adslports_inv = {}
vdslports = {}
vdslports_inv = {}



def daemonize():
    try:
        pid = os.fork()
    except OSError, e:
        raise Exception, "%s [%d]" % (e.strerror, e.errno)
    if pid == 0:    #first child
        os.setsid()
        try:
            pid = os.fork()
        except OSError, e:
            raise Exception, "%s [%d]" % (e.strerror, e.errno)
        if pid == 0:
            os.chdir('/')
            os.umask(0)
        else:
            subprocess.call("echo '%s' > /var/run/trap-handler.pid"%pid, shell=True)
            os._exit(0)
    else:
        os._exit(0)
    import resource
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = MAXFD
    for fd in range(0, maxfd):
         try:
             os.close(fd)
         except OSError:	# ERROR, fd wasn't open to begin with (ignored)
             pass
    os.open('/dev/null', os.O_RDWR)
    os.dup2(0, 1)
    os.dup2(0, 2)
    return 0



def getDslams():
    query = """select dslam_id,ime,dslam_type_id,ip from dslami where dslam_type_id >= 1"""
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        dslams[row[0]] = {'dslam_type_id': row[2], 'name': row[1], 'ip': row[3]}
        dslams_inv[row[1]] = {'id': row[0], 'dslam_type_id': row[2], 'ip': row[3]}



def getPorts():
    query = """select id,name,ifIndex from ports where type=1"""
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        adslports[row[0]] = {'name': row[1], 'ifIndex': row[2]}
        adslports_inv[row[1]] = {'id': row[0]}
        ports[row[0]] = {'name': row[1], 'ifIndex': row[2]}
    query = """select id,name,ifIndex from ports where type=2"""
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        ports[row[0]] = {'name': row[1], 'ifIndex': row[2]}
        vdslports[row[0]] = {'name': row[1]}
        vdslports_inv[row[1]] = {'id': row[0]}



def getBoardVersion(ip, slot):
    key = "boards:%s:0/%s"%(ip,slot)
    boardVersion = red_boards.get(key)
    if boardVersion:
        return boardVersion
    hwSlotVersion = (1,3,6,1,4,1,2011,6,3,3,2,1,5) + (0,int(slot))
    errorIndication, errorStatus, errorIndex, varBinds = cmdGen.getCmd(
      cmdgen.CommunityData('my-agent', community),
      cmdgen.UdpTransportTarget((ip, 161)),
      hwSlotVersion
    )
    if errorStatus != 0:
        return 1
    result = varBinds[0][1]._value
    m = re.search("Version: ([0-9A-Z]+) ", result)
#ili re.search("Main Board: ([0-9A-Z]+)", result)
    boardVersion = m.group(1)
    return boardVersion


def get_inventory(event):
    dslam_id = event['dslam_id']
    frame = event['frame']
    slot = event['slot']
    port = event['port']
    dslam_name = event['dslam_name']
    port_name = '%s/%s/%s'%(frame,slot,port)
    ip = event['ip']
    gevent.sleep(5)
    logger.debug("%s %s: get_inventory"%(dslam_name, port_name))
    if event['dslam_type_id'] == 1:
        system_vendor_id = ".1.3.6.1.2.1.10.94.1.1.3.1.2."
        inv_version_num = ".1.3.6.1.2.1.10.94.1.1.3.1.3."
        inv_serial_num = ".1.3.6.1.2.1.10.94.1.1.3.1.1."
        boardType = getBoardVersion(ip, int(slot))
        port_name = "%s/%s/%s"%(frame,slot,port)
        if boardType in ma5600BoardTypes:
            ifIndex = 201326592 + 8192*int(slot) + 64*int(port)
            port_id = adslports_inv[port_name]['id']
        else:
            return -1
        system_vendor_id += str(ifIndex)
        inv_version_num += str(ifIndex)
        inv_serial_num += str(ifIndex)
        varlist = []
        varlist.append(netsnmp.Varbind(system_vendor_id))
        varlist.append(netsnmp.Varbind(inv_version_num))
        varlist.append(netsnmp.Varbind(inv_serial_num))
        data = netsnmp.snmpget(*varlist, Version=2, DestHost=event['ip'], Community=community, Retries=1, Timeout=2000000)  #, UseNumeric=1)
        if data[0] is not None:
            query = """select id, time,system_vendor_id,inv_version_num,inv_serial_num from 
                    dsl_line_inventory where dslam_id=%s and port_id=%s and time = 
                    (select max(time) from dsl_line_inventory where dslam_id=%s and port_id=%s)"""%(dslam_id,port_id,dslam_id,port_id)
            cursor.execute(query)
            row = cursor.fetchone()
            if row is None or data[2] != row[4]:
                if row is not None and data[2] is None and row[4] == 'None':
                    return 
                query = """insert into dsl_line_inventory (dslam_id, port_id, system_vendor_id,
                    inv_version_num,inv_serial_num) values (%s,%s,%s,%s,%s)"""
                insert_data = [(dslam_id,port_id,data[0],data[1],data[2])]
                cursor.executemany(query,insert_data)
                logger.debug(data)
                logger.debug("Updating inventory on %s %s"%(event['dslam_name'], ports[port_id]['name']))
                portinventory = {
                    'system_vendor_id': data[0],
                    'inv_version_num': data[1],
                    'inv_serial_num': data[2],
                    'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                print data
                red_boards.hset("inventory:%s"%ip, port_id, json.dumps(portinventory))

    elif event['dslam_type_id'] in (2,3,4,5):
        vdsl_g994_vendor_id = ".1.3.6.1.4.1.2011.6.115.1.3.1.1.2."
        vdsl_system_vendor_id = ".1.3.6.1.4.1.2011.6.115.1.3.1.1.3."
        vdsl_inv_version_num = ".1.3.6.1.4.1.2011.6.115.1.3.1.1.4."
        vdsl_inv_serial_num = ".1.3.6.1.4.1.2011.6.115.1.3.1.1.5."
        adsl_g994_vendor_id = ".1.3.6.1.2.1.10.238.1.3.1.1.2."
        adsl_system_vendor_id = ".1.3.6.1.2.1.10.238.1.3.1.1.3."
        adsl_inv_version_num = ".1.3.6.1.2.1.10.238.1.3.1.1.4."
        adsl_inv_serial_num = ".1.3.6.1.2.1.10.238.1.3.1.1.5." 
        varlist = []
        boardType = getBoardVersion(ip, int(slot))
        port_name = "%s/%s/%s"%(frame,slot,port)
        if boardType in adslBoardTypes:
            ifIndex = 201326592 + 8192*int(slot) + 64*int(port)
            port_id = adslports_inv[port_name]['id']
            g994_vendor_id = adsl_g994_vendor_id + str(ifIndex) + ".2"
            system_vendor_id = adsl_system_vendor_id + str(ifIndex) + ".2"
            inv_version_num = adsl_inv_version_num + str(ifIndex) + ".2"
            inv_serial_num = adsl_inv_serial_num + str(ifIndex) + ".2"
            varlist.append(netsnmp.Varbind(g994_vendor_id))
            varlist.append(netsnmp.Varbind(system_vendor_id))
            varlist.append(netsnmp.Varbind(inv_version_num))
            varlist.append(netsnmp.Varbind(inv_serial_num))
        elif boardType in vdslBoardTypes:
            ifIndex = 4160749568 + 8192*int(slot) + 64*int(port)
            port_id = vdslports_inv[port_name]['id']
            g994_vendor_id = vdsl_g994_vendor_id + str(ifIndex) + ".2"
            system_vendor_id = vdsl_system_vendor_id + str(ifIndex) + ".2"
            inv_version_num = vdsl_inv_version_num + str(ifIndex) + ".2"
            inv_serial_num = vdsl_inv_serial_num + str(ifIndex) + ".2"
            varlist.append(netsnmp.Varbind(g994_vendor_id))
            varlist.append(netsnmp.Varbind(system_vendor_id))
            varlist.append(netsnmp.Varbind(inv_version_num))
            varlist.append(netsnmp.Varbind(inv_serial_num))
        data = netsnmp.snmpget(*varlist, Version=2, DestHost=ip, Community=community, Retries=1, Timeout=2000000)
        if data[0] is not None:
            query = """select id, time,system_vendor_id,inv_version_num,inv_serial_num from 
                    dsl_line_inventory where dslam_id=%s and port_id=%s and time = 
                    (select max(time) from dsl_line_inventory where dslam_id=%s and port_id=%s)"""%(dslam_id,port_id,dslam_id,port_id)
            cursor.execute(query)
            row = cursor.fetchone()
            if row is None or data[3] != row[4]:
                if row is not None and data[3] is None and row[4] == 'None':
                    return
                query = """insert into dsl_line_inventory (dslam_id,port_id,g994_vendor_id,
                    system_vendor_id,inv_version_num,inv_serial_num) values (%s,%s,%s,%s,%s,%s)"""
                insert_data = [(dslam_id,port_id,binascii.hexlify(data[0]),binascii.hexlify(data[1]),data[2],data[3])]
                cursor.executemany(query,insert_data)
                logger.debug(data)
                logger.debug("Updating inventory on %s %s"%(event['dslam_name'], ports[port_id]['name']))
                g994_vendor_id = binascii.hexlify(data[0])
                system_vendor_id = binascii.hexlify(data[1])
                try:
                    json.dumps(data[2])
                    inv_version_number = data[2]
                except TypeError:
                    inv_version_number = binascii.hexlify(data[2])
                portinventory = {
                    'g994_vendor_id': g994_vendor_id,
                    'system_vendor_id': system_vendor_id,
                    'inv_version_num': inv_version_num,
                    'inv_serial_num': data[3],
                    'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                red_boards.hset("inventory:%s"%ip, port_id, json.dumps(portinventory))



def get_last_retrain_info(msg):
    if msg['dslam_type_id'] in (2,3,4):
        logger.debug("%s 0/%s/%s: get last retrain info"%(msg['dslam_name'], msg['slot'], msg['port']))
        boardType = getBoardVersion(msg['ip'], int(msg['slot']))
        if boardType in vdslBoardTypes:
            ifIndex = 4160749568 + 8192*int(msg['slot']) + 64*int(msg['port'])
            oid = "%s.%s"%(vdsl2LineStatusLastRetrainInfo,ifIndex)
        elif boardType in adslBoardTypes:
            ifIndex = 201326592 + 8192*int(msg['slot']) + 64*int(msg['port'])
            oid = "%s.%s"%(hwadsl2LineStatusLastRetrainInfo,ifIndex)
        else:
            return 0
        errorIndication, errorStatus, errorIndex, varBinds = cmdGen.getCmd(
          cmdgen.CommunityData('my-agent', community),
          cmdgen.UdpTransportTarget((msg['ip'], 161)),
          oid
        )
        if errorStatus != 0:
            logger.error('Error getting last retrain info: %s 0/%s/%s'%(ip,slot,port))
            return -1
        return varBinds[0][1]._value  



def line_profile_change(event):
    ip = event['ip']
    dslam_type_id = event['dslam_type_id']
    dslam_name = event['dslam_name']
    frame = event['frame']
    slot = event['slot']
    port = event['port']
    port_name = frame+'/'+slot+'/'+port
    if red_boards.hexists('set:profile:'+ip, port_name):
        # we should change profile on this port
        profile_str = red_boards.hget('set:profile:'+ip, port_name) 
        profile = json.loads(profile_str)
        if dslam_type_id == 1:
            if 'line_profile' in profile:
                line_profile = profile['line_profile']
                status = ma5600.setLineProfile(ip,int(frame),int(slot),int(port),line_profile)
                if status == 0:
                    logger.debug("%s %s: set line_profile %s"%(ip, port_name, line_profile))
                    red_boards.hdel("set:profile:"+ip, port_name)
                    return status
                else:
                    logger.debug("%s %s: error set line_profile %s"%(ip, port_name, line_profile))
                    red_boards.hdel("set:profile:"+ip, port_name)
                    return status
            if 'extended_profile' in profile:
                extended_profile = profile['extended_profile']
                ma5600.setExtLineProfile (ip,int(frame),int(slot),int(port),extended_profile)
        elif dslam_type_id in (2,3,4):
            try:
                status = ma5600t.setXdslVOPProfile(ip,int(frame),int(slot),int(port),**profile)
                if status == 0:
                    logger.debug("%s %s: set line_profile %s"%(dslam_name, port_name, profile_str))
                    red_boards.hdel("set:profile:"+ip, port_name)
                    return status
                else:
                    logger.debug("%s %s: error set line_profile %s"%(dslam_name, port_name, profile_str))
                    red_boards.hdel("set:profile:"+ip, port_name)
                    return status
            except Exception, e:
                logger.error("%s %s: setXdslVOPProfile exception %s: %s"%(ip, port_name, type(e).__name__, e))
    else:
        return 0



def handler_activated(event):
#    gevent.spawn(get_inventory, event)
    get_inventory(event)



def handler_activating(event):
    line_down = float(event['time']) - time.time() 
    if line_down < 60:
        last_retrain = get_last_retrain_info(event)
    profile_change = line_profile_change(event)
#    srv_profile = srv_profile_change(event)
#    if not profile_change:
#        check_error_state(event)



def get_lines():
    while(1):
        queue, eventstr = red_events.brpop("events")
        event = json.loads(eventstr)
        if event['event_id'] == '10':
            g = gevent.Greenlet.spawn(handler_activating, event)
#            handler_activating(event)
#            g.join(timeout=0)
        elif event['event_id'] == '1':
            g = gevent.Greenlet.spawn(handler_activated, event)
#            g.join(timeout=0)


if __name__ == '__main__':
    logger.info("Starting program")
    getDslams()
    getPorts()
    get_lines()

