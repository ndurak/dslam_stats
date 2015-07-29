#!/usr/bin/python

from pysnmp.carrier.asynsock.dispatch import AsynsockDispatcher
from pysnmp.carrier.asynsock.dgram import udp
from pyasn1.codec.ber import decoder
from pysnmp.proto import api
import subprocess
import os
import MySQLdb
import logging
import re
import redis
import json
import time

MAXFD = 1024


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


def cbFun(transportDispatcher, transportDomain, transportAddress, wholeMsg):
    while wholeMsg:
        msgVer = int(api.decodeMessageVersion(wholeMsg))
        if api.protoModules.has_key(msgVer):
            pMod = api.protoModules[msgVer]
        else:
            logger.error('Unsupported SNMP version %s' % msgVer)
            return
        reqMsg, wholeMsg = decoder.decode(
            wholeMsg, asn1Spec=pMod.Message(),
            )
        ip = transportAddress[0]
	query = """ select * from dslami where ip='%s'"""%(ip)
        cursor.execute(query)
        row = cursor.fetchone()
        if row == None:
            continue
        else:
            dslam_id = row[0]
            dslam_type_id = row[2]
            name = row[1]
        logger.debug('Notification message from %s'%name)
        reqPDU = pMod.apiMessage.getPDU(reqMsg)
        if reqPDU.isSameTypeWith(pMod.TrapPDU()):
            if msgVer == api.protoVersion1:
                varBinds = pMod.apiTrapPDU.getVarBindList(reqPDU)
            else:
                varBinds = pMod.apiPDU.getVarBindList(reqPDU)
            for varBind in varBinds:
                logger.debug("%s: %s"%(varBind[0],varBind[1].getComponent().getComponent().prettyPrint()))
            try:
                trapId = varBinds[2][1].getComponent().getComponent().prettyPrint()
            except IndexError, e:
                logger.critical("varBinds[2][1] List Index out of range: %s"%e)
                logger.debug(varBinds)
                return
            if trapId in ('21','22','24', '73'):
#            oidlist = varBinds[6][0].prettyPrint().split('.')
                frame = varBinds[3][1].getComponent().getComponent().prettyPrint()
                slot = varBinds[4][1].getComponent().getComponent().prettyPrint()
                port = varBinds[5][1].getComponent().getComponent().prettyPrint()
                event_id = varBinds[6][1].getComponent().getComponent().prettyPrint()
                query = """ insert into dslam_events (dslam_id, frame, slot, port, event) \
                        values (%s, %s, %s, %s, %s)"""%(dslam_id,frame,slot,port,event_id)
                cursor.execute(query)
                msg = {
                  'lastrow': cursor.lastrowid,
                  'dslam_id': dslam_id,
                  'dslam_type_id': dslam_type_id,
                  'dslam_name': name,
                  'ip': ip,
                  'frame': frame,
                  'slot': slot,
                  'port': port,
                  'event_id': event_id,
                  'time': time.time()
                }
                red1.lpush('events',json.dumps(msg))
            elif trapId == '15':
                # Add srv port
                try:
                    hwExtSrvFlowIndex = varBinds[5][1].getComponent().getComponent().prettyPrint()
                    hwExtSrvFlowSrvType = varBinds[6][1].getComponent().getComponent().prettyPrint()
                    hwExtSrvFlowUserPara = varBinds[7][1].getComponent().getComponent().prettyPrint()
                    hwExtSrvFlowPara1 = varBinds[8][1].getComponent().getComponent().prettyPrint()   # 1 if ptm, ifIndex if atm
                    hwExtSrvFlowPara2 = varBinds[9][1].getComponent().getComponent().prettyPrint()   # frame if ptm, vpi if atm
                    hwExtSrvFlowPara3 = varBinds[10][1].getComponent().getComponent().prettyPrint()   # slot if ptm, vci if atm
                    hwExtSrvFlowPara4 = varBinds[11][1].getComponent().getComponent().prettyPrint()   # port if ptm, -1 if atm
                    paraType = varBinds[13][1].getComponent().getComponent().prettyPrint()   # 3 if ptm, 7 if atm
                    vlanid = varBinds[14][1].getComponent().getComponent().prettyPrint()
                    rxTrafficId = varBinds[15][1].getComponent().getComponent().prettyPrint()
                    txTrafficId = varBinds[16][1].getComponent().getComponent().prettyPrint()
#                    rxProfile = varBinds[19][1].getComponent().getComponent().prettyPrint()
#                    txProfile = varBinds[20][1].getComponent().getComponent().prettyPrint()
                except Exception, e:
                    logger.debug("Exception %s: %s"%(type(e).__name__, e))
                    logger.debug(len(varBinds))
                    for oid, val in varBinds:
                        oidstr = oid.prettyPrint()
                        valstr = val.getComponent().getComponent().prettyPrint()
                        logger.debug("    %s: %s"%(oidstr, valstr)) 
                    return
                if paraType == '3':
                    # vdsl ptm
                    logger.info("%s %s/%s/%s:%s rx=%s, tx=%s added"
                        %(name, '0', hwExtSrvFlowPara3, hwExtSrvFlowPara4, hwExtSrvFlowUserPara, rxTrafficId, txTrafficId))
                elif paraType == '7':
                    # atm
                    slot = (long(hwExtSrvFlowPara1) - 201326592) / 8192
                    port = ((long(hwExtSrvFlowPara1) - 201326592) % 8192) / 64
                    dslam_port = '0/'+str(slot)+'/'+str(port)
                    logger.info("%s %s:%s.%s rx=%s, tx=%s added"
                        %(name, dslam_port, hwExtSrvFlowPara2, hwExtSrvFlowPara3, rxTrafficId, txTrafficId))
            elif trapId == '423':
                hwExtSrvFlowIndex = varBinds[4][1].getComponent().getComponent().prettyPrint()
                logger.info("%s deleted %s"%(name, hwExtSrvFlowIndex))
                redpipe.srem("dslams:"+ip+":hwExtSrvFlowPara2:0", hwExtSrvFlowIndex)
                redpipe.srem("dslams:"+ip+":hwExtSrvFlowMultiServiceUserPara:32", hwExtSrvFlowIndex)
                redpipe.srem("dslams:"+ip+":hwExtSrvFlowMultiServiceUserPara:56", hwExtSrvFlowIndex)
                redpipe.srem("dslams:"+ip+":hwExtSrvFlowMultiServiceUserPara:81", hwExtSrvFlowIndex)
                redpipe.srem("dslams:"+ip+":hwExtSrvFlowMultiServiceUserPara:86", hwExtSrvFlowIndex)
                redpipe.execute()
#            elif trapId = '2':
                # old ma5600 chasis srv port event
#                ifIndex = varBinds[-3][1].getComponent().getComponent().prettyPrint()
#                vpi = varBinds[-2][1].getComponent().getComponent().prettyPrint()
#                vci = varBinds[-1][1].getComponent().getComponent().prettyPrint()
            elif trapId == '12':
                # MELT
                ifIndex = varBinds[4][1].getComponent().getComponent().prettyPrint()
                logger.debug("%s %s: MELT",name, ifIndex)
                melt_data = []
                for varbind in varBinds[5:]:
                    melt_data.append((varbind[0].prettyPrint(), varbind[1].getComponent().getComponent().prettyPrint()))
                msg = {
                    'dslam_id': dslam_id,
                    'ifIndex': ifIndex,
                    'melt_data': melt_data
                }
                logger.debug(melt_data)
                red1.lpush("melt", json.dumps(msg))
            else:
                logger.debug("Unknown mib var detected dslam=%s"%name)
#                for oid, val in varBinds:
#                    oidstr = oid.prettyPrint()
#                    valstr = val.getComponent().getComponent().prettyPrint()
#                    logger.debug("    %s: %s"%(oidstr, valstr))
                return

    return wholeMsg

if __name__ == "__main__":
    daemonize()
    FORMAT = "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s"
#    FORMAT = '%(name)s %(asctime)s %(levelname)s %(message)s'
    logfile = logging.basicConfig(filename='/var/log/apps/huawei-traps/handler.log', level=logging.DEBUG, format=FORMAT)
    logger = logging.getLogger("Huawei")
    logger.info("Started")
    conn = MySQLdb.connect(host = "mysqldb.net",
                      user = "user",
                      passwd = "pw123",
                      db = "dslam")
    conn.autocommit(True)
    cursor = conn.cursor()
    red = redis.StrictRedis(host='redishost.myorg.net', port=6379)
    redpipe = red.pipeline()
    red1 = redis.StrictRedis(host='redishost.myorg.net', port=6379, db=1)
    transportDispatcher = AsynsockDispatcher()
    transportDispatcher.registerTransport(
        udp.domainName, udp.UdpSocketTransport().openServerMode(('', 162))
        )
    transportDispatcher.registerRecvCbFun(cbFun)
    transportDispatcher.jobStarted(1)
    transportDispatcher.runDispatcher()

