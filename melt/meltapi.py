import MySQLdb
import logging
import re
from django.db import connection, connections
from pysnmp.entity.rfc3413.oneliner import cmdgen
from pysnmp.proto import rfc1902

cmdGen = cmdgen.CommandGenerator()

logger = logging.getLogger('melt')

hwLoopLineTestOprType = ".1.3.6.1.4.1.2011.6.21.1.1.1.2.1.1."
community = "public"

def start_melt(params):
    cursor = connection.cursor()
    if 'ip' in params:
        query = "select dslam_id, dslam_type_id, name from dslami where ip='%s'"%params['ip']
        cursor.execute(query)
        row = cursor.fetchone()
        ip = params['ip']
        dslam_id = row[0]
        dslam_type_id = row[1]
        dslam_name = row[2]
    elif 'dslam_id' in params:
        query = "select ip, dslam_type_id, name from dslami where dslam_id=%s"%params['dslam_id']
        cursor.execute(query)
        row = cursor.fetchone()
        ip = row[0]
        dslam_id = params['dslam_id']
        dslam_type_id = row[1]
        dslam_name = row[2]
    elif 'dslam_name' in params:
        query = "select ip, dslam_type_id, dslam_id from dslami where name='%s'"%params['dslam_name']
        cursor.execute(query)
        row = cursor.fetchone()
        dslam_name = params['dslam_name']
        ip = row[0]
        dslam_type_id = row[1]
        dslam_id = row[2]
    else:
        return -32602

    if 'port_id' in params:
        port_id = params['port_id']
        query = "select ifIndex, name from ports where id=%s"%port_id 
        cursor.execute(query)
        row = cursor.fetchone()
        if row is None:
            return -32602
        ifIndex = row[0]
        port_name = row[1]
    elif 'port_name' in params:
        port_name = params['port_name']
        query = "select ifIndex, id from ports where type = 2 and name='%s'"%port_name
        cursor.execute(query)
        row = cursor.fetchone()
        if row is None:
            return -32602
        ifIndex = row[0]
        port_id = row[1]
    else:
        return -32602

    if 'community' in params:
        community = 'public'

    errorIndication, errorStatus, errorIndex, varBinds = cmdGen.setCmd(
      cmdgen.CommunityData('my-agent', community),
      cmdgen.UdpTransportTarget((ip, 161)),
      (hwLoopLineTestOprType + str(ifIndex), rfc1902.Integer(2))
    )
    if errorStatus == 0:
        if errorIndication is None:
            logger.debug("MELT: %s %s"%(dslam_name, port_name))
            return 0
        elif re.search("No SNMP response", str(errorIndication)):
            logger.warning("No SNMP response from %s"%dslam_name)
            return -32001
        else:
            logger.error("%s %s: %s"%(dslam_name, port_name, errorIndication))
            return -32000
    else:
        return -32000

