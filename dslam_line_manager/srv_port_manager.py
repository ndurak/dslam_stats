#!/usr/bin/python

import sys
from dsl_line_manager import *
import logging
import time

sys.path.append('/opt/apps/dslam_stats/DslamManager/')
import Ma5600SnmpApi
import Ma5600TSnmpApi

logger = logging.getLogger("SrvPortManager")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s")
fh = logging.FileHandler("/var/log/apps/dslam_line_manager/srv_port_manager.log")
fh.setFormatter(formatter)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.WARNING)
logger.addHandler(fh)
logger.addHandler(ch)

redisExp = 86800

traffic_table = {}
traffic_table_inv = {}

def getTrafficProfiles():
    query = "select id,name from traffic_rucno"
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        traffic_table[row[0]] = row[1]
        traffic_table_inv[row[1]] = row[0]


def srv_port_manager(dslamportkey):
    userdata = userports[dslamportkey]
    dslam_id = dslamportkey >> 14
    port_id = dslamportkey % (dslam_id<<14)
    ip = dslams[dslam_id]['ip']
    dslam = dslams[dslam_id]['ime']
    dslam_type_id = dslams[dslam_id]['dslam_type_id']
    port_name = ports[port_id]['name']
    frame, slot, port = port_name.split('/')
    servicename = userports[dslamportkey]['servicename']
    dataspeedcode =userports[dslamportkey]['dataspeedcode']
    voicespeedcode = userports[dslamportkey]['voicespeedcode']
    key = "%s:%s:%s"%(servicename,dataspeedcode,voicespeedcode)
    try:
        services[key]
    except KeyError:
        if dslam_type_id in (2,3,4):
            # ovo zbog toga sto nas obss stavlja voicespeedcode=NULL za VDSL* 
            # servicecode (prov.dslam_nbi_code_table tablica)
            key = "%s:%s:None"%(servicename,dataspeedcode)
            if key not in services:
                self.logger.warning("""Unknown service for %s %s servicename=%s dataspeedcode=%s voicespeedcode=%s"""
                    %(dslams[dslam_id]['ime'],ports[port_id]['name'],servicename,dataspeedcode,voicespeedcode))
                return None
        else:
            logger.warning("""Unknown service for %s %s servicename=%s dataspeedcode=%s voicespeedcode=%s"""
                %(dslams[dslam_id]['ime'],ports[port_id]['name'],servicename,dataspeedcode,voicespeedcode))
            return None
    profile_payed_ds = services[key]['traffic_ds'].rstrip()
    profile_payed_us = services[key]['traffic_us'].rstrip()
    if profile_payed_ds == 'DSL_0_rx':
        return None
    if dslams[dslam_id]['dslam_type_id'] == 1:
        srvports = ma5600.getSrvPortData(ip, int(frame), int(slot), int(port))
        for srvport in srvports:
            if int(srvport['vci']) == 86:
                rx = srvport['rx']
                tx = srvport['tx']
                profile_payed_ds_id = traffic_table_inv[profile_payed_ds]
                try:
                    profile_ds = traffic_table[rx]
                except KeyError:
                    print dslam, port_name
                profile_payed_us_id = traffic_table_inv[profile_payed_us]
                try:
                    profile_us = traffic_table[tx]
                except KeyError:
                    print dslam, port_name
                if rx != profile_payed_ds_id:
                    # put srvport in queue for deleting
                    redpipe.hset('set:srvport:%s'%dslam, port_name, srvport)
                    redpipe.expire('set:srvport:%s'%dslam, redisExp)
                elif tx != profile_payed_us_id:
                    # put srvport in queue for deleting
                    redpipe.hset('set:srvport:%s'%dslam, port_name, srvport)
                    redpipe.expire('set:srvport:%s'%dslam, redisExp)


if __name__ == '__main__':
    getDslams()              # dslams, dslams_inv
    getPorts()               # ports, ports_inv
    getProtected()           # protected
    getUsers()               # userports
    bindServices2Profiles()
    getTrafficProfiles()
    ma5600 = Ma5600SnmpApi.Ma5600SnmpApi()
    ma5600t = Ma5600TSnmpApi.Ma5600TSnmpApi()
    for dslamportkey in userports:
        if dslamportkey in protectedPorts:
            continue
        srv_port_manager(dslamportkey)
#    redpipe.execute()
