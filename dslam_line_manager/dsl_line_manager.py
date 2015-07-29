###!/usr/bin/python
import sys
import dslamdb, provdb
import logging
import sys
import re
import redis
from ConfigParser import *
import json

config = ConfigParser()
config.read("dsl_line_manager.conf")
logfile = config.get('Logging', 'logfile')
loglevel = config.get('Logging', 'loglevel')

conn1, cursor1 = dslamdb.dslamconnect()
conn2, cursor2 = provdb.provconnect()
red = redis.StrictRedis(host="dslam.zg.iskon.hr",db=2)
redpipe = red.pipeline()
#logging.basicConfig(filename=DLOG,level=logging.DEBUG,format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("LineManager")
logger.setLevel(loglevel)
formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s")
fh = logging.FileHandler(logfile)
fh.setFormatter(formatter)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.WARNING)
logger.addHandler(fh)
logger.addHandler(ch)

min_data_rate = float(config.get('Parameters', 'min_data_rate'))

userports = {}       # dict for keeping service info data for particular user/port
portsstatus = {}     # data from dslam_ports_status table
alluserstats = {}    # current profile, rates ...
services = {}
dslams = {}
dslams_inv = {}
ports = {}
adslports = {}
adslports_inv = {}
vdslports = {}
vdslports_inv = {}
protectedPorts = {}



#####################################################################
# Gets all dslams from db, and builds dslams and dslams_inv dict
#####################################################################
def getDslams():
    query = """select dslam_id,ime,dslam_type_id,ip from dslami where dslam_type_id >= 1"""
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        dslams[row[0]] = {'dslam_type_id': row[2], 'ime': row[1], 'ip': row[3]}
        dslams_inv[row[1]] = {'id': row[0], 'dslam_type_id': row[2], 'ip': row[3]}



#####################################################################
# Gets all ports from db and builds ports and ports_inv dict
#####################################################################
def getPorts():
    query = """select id,name from ports where type=1"""
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        adslports[row[0]] = {'name': row[1]}
        adslports_inv[row[1]] = {'id': row[0]}
        ports[row[0]] = {'name': row[1]}
    query = """select id,name from ports where type=2"""
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        ports[row[0]] = {'name': row[1]}
        vdslports[row[0]] = {'name': row[1]}
        vdslports_inv[row[1]] = {'id': row[0]}



#####################################################################
# Builds protectedPorts dict
#####################################################################
def getProtected():
    query = """select dslam_id,port_id,unos from dsl_protected_ports"""
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        dslam_id = row[0]
        port_id = row[1]
        dslamportkey = (dslam_id<<14) + port_id
        protectedPorts[dslamportkey] = row[2]



#####################################################################
# Get all users from prov db; builds userports dict
#####################################################################
def getUsers():
    global cursor2
    query = """select dslam_id, frame, slot, port, d.servicename, dataspeedcode, 
          voicespeedcode, d.addstbs, d.opthd, productclass,d.updated from pso_dslam d, pso_hdm h 
          where d.code=h.code and d.ASSIGNEDPORT is not null and d.state=1 and 
          d.SERVICENAME in ('CARNET','DUO','DUODATATV','DUOVOICETV','SHARED',
          'SOLO','SOLODATA','TRIO','TRIOCARNET','VDSLDATA','VDSLDATATV','VDSLDUO',
          'VDSLTRIO')"""
    cursor2.execute(query)
    rows = cursor2.fetchall()
    for row in rows:
        dslam = row[0]
        port = "%s/%s/%s"%(row[1],row[2],row[3])
        board = "%s/%s"%(row[1],row[2])
        try:
            dslam_id = dslams_inv[dslam]['id']
        except KeyError:
            continue
        if dslams[dslam_id]['dslam_type_id'] == 1:
            port_id = adslports_inv[port]['id']
        elif dslams[dslam_id]['dslam_type_id'] in (2,3,4):
            dslam_ip = dslams[dslam_id]['ip']
            board_type = red.get("boards:%s:%s"%(dslam_ip,board))
            if board_type in Ma5600T.adslBoardTypes or board_type in Ma5600.adslBoardTypes:
                port_id = adslports_inv[port]['id']
            elif board_type in Ma5600T.vdslBoardTypes:
                port_id = vdslports_inv[port]['id']
            else:
                logger.warning("Unknown board type %s on %s %s"%(board_type,dslam,port))
                continue
        else:
            logger.warning("Unknown dslam type")
            continue
        dslamportkey = (dslam_id<<14) + port_id 
        userports[dslamportkey]={
          'dslam_id': dslam_id,
          'port_id': port_id,
          'servicename': row[4],
          'dataspeedcode': row[5],
          'voicespeedcode': row[6],
          'addstbs': row[7],
          'opthd': row[8],
          'productclass': row[9]
        }
        try:
            vrijeme_unosa = protectedPorts[dslamportkey]
            if vrijeme_unosa < row[10]:
                del protectedPorts[dslamportkey]
        except KeyError:
            pass
    # find ports with more active accounts 
    query = """ select dslam_id,frame,slot,port,count(*) from pso_dslam d, pso_hdm h where 
        d.code=h.code and d.ASSIGNEDPORT is not null and d.state=1 and d.SERVICENAME in 
        ('CARNET','DUO','DUODATATV','DUOVOICETV','SHARED','SOLO','SOLODATA','TRIO',
        'TRIOCARNET','VDSLDATA','VDSLDATATV','VDSLDUO','VDSLTRIO') group by dslam_id,frame,
        slot,port having count(*) > 1""" 
    cursor2.execute(query)
    rows = cursor2.fetchall()
    for row in rows:
        dslam = row[0]
        port = "%s/%s/%s"%(row[1],row[2],row[3])
        dslam_id = dslams_inv[dslam]['id']
        if dslams[dslam_id]['dslam_type_id'] == 1:
            port_id = adslports_inv[port]['id']
        elif dslams[dslam_id]['dslam_type_id'] in (2,3,4):
            dslam_ip = dslams[dslam_id]['ip']
            board_type = red.get("boards:%s:%s"%(dslam_ip,port))
            if board_type in Ma5600T.adslBoardTypes or board_type in Ma5600.adslBoardTypes:
                port_id = adslports_inv[port]['id']
            elif board_type in Ma5600T.vdslBoardTypes:
                port_id = vdslports_inv[port]['id']
            else:
                logger.warning("Unknown board type %s"%board_type)
                continue
        else:
            logger.warning("Unknown dslam type")
            continue
        dslamportkey = (dslam_id<<14) + port_id 
        userports[dslamportkey]['usercount'] = row[4]
        logger.warning("Prov db: %s active users on %s %s"%(row[4],dslam,port))
        del(userports[dslamportkey])




#####################################################################
# Gets service profile data from prov db and builds services dict
#####################################################################
def bindServices2Profiles():
    global cursor2
    query = """select service_code,data_speed_code,voice_speed_code,vdsl,setup_data,
            setup_iptv,setup_voice,xdls_profile_ds_rate,xdsl_profile_us_rate,
            traffic_attr_data_rx,TRAFFIC_ATTR_DATA_TX,TRAFFIC_ATTR_VOICE_RX,
            TRAFFIC_ATTR_VOICE_TX from prov.dslam_nbi_code_table where XDLS_PROFILE_DS_RATE is not NULL"""
    cursor2.execute(query)
    rows = cursor2.fetchall()
    for row in rows:
        servicename = row[0]
        dataspeedcode = row[1]
        voicespeedcode = row[2]
        vdsl = row[3]
        data = row[4]
        iptv = row[5]
        voip = row[6]
        profile_ds = row[7]
        m = re.search("(\d+)(M|K)$", profile_ds)
        if m.group(2) == "M":
            rate_ds = int(m.group(1)) * 1200
            rate_ptm_ds = int(m.group(1)) * 1024
        elif m.group(2) == "K":
            rate_ds = int(m.group(1))
            rate_ptm_ds = int(m.group(1))
        profile_us = row[8]
        m = re.search("(\d+)(M|K)$", profile_us)
        if m.group(2) == "M":
            rate_us = int(m.group(1)) * 1024
            rate_ptm_us = int(m.group(1)) * 1024
        elif m.group(2) == "K":
            rate_us = int(m.group(1))
            rate_ptm_us = int(m.group(1))
        traffic_ds = row[9]
        traffic_us = row[10]
        if iptv and data:
            min_rate_ds = (rate_ds - 3600) / 2
            min_rate_ptm_ds = (rate_ds - 3072) / 2
        elif data:
            min_rate_ds = rate_ds / 2
            min_rate_ptm_ds = min_rate_ds
        else:
            min_rate_ds = rate_ds*9/10 
            min_rate_ptm_ds = rate_ptm_ds
        if data:
            min_rate_us = rate_us / 2
            min_rate_ptm_us = rate_ptm_us / 2
        else:
            min_rate_us = rate_us
            min_rate_ptm_us = rate_ptm_us
        key = "%s:%s:%s"%(servicename,dataspeedcode,voicespeedcode)
        services[key] = { 'profile_ds': profile_ds, 'profile_us': profile_us, 
                          'rate_ds': rate_ds, 'rate_us': rate_us, 
                          'rate_ptm_ds': rate_ptm_ds, 'rate_ptm_us': rate_ptm_us,
                          'min_rate_ds': min_rate_ds, 'min_rate_us': min_rate_us,
                          'min_rate_ptm_ds': min_rate_ptm_ds, 'min_rate_ptm_us': min_rate_ptm_us,
                          'iptv': iptv, 'data': data, 'voip': voip, 'vdsl': vdsl,
                          'traffic_ds': traffic_ds, 'traffic_us': traffic_us,
                        }

    return rows




def getBoardType(dslam_id,port_id):
    ip = dslams[dslam_id]['ip']
    port_name = ports[port_id]['name']
    frame,slot,port=port_name.split("/")
    board = "%s/%s"%(frame,slot)
    return red.get("boards:%s:%s"%(ip,board))
    global cursor1
    query = """select dslam_id, board, type from dslam_boards"""
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        dslam_id = row[0]
        port_id = row[1]
        dslamportkey = (dslam_id<<14) + port_id 
        boardtype[dslamportkey] = row[2]

 

#
#trenutni profil koji useri imaju / trenutno stanje parice
#
def getAllUserCurrent():
    global cursor1
    # MA5600
    query = """select a.dslam_id, a.port_id, a.line_profile_index, p.max_dl, p.max_upl, extended_profile_index, \
            u_noise_marg, d_noise_marg, max_u_rate, max_d_rate, u_rate, d_rate, d_INP, u_INP, d_stream_atten,\
            u_stream_atten from adsl_operation a left join profili p on a.line_profile_index=p.line_profile_index \
            where date_time_id=(select max(date_time_id)from date_time )-1"""
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        dslam_id = row[0]
        port_id = row[1]
        dslamportkey = (dslam_id<<14) + port_id
        alluserstats[dslamportkey] = {
          'line_profile': row[2],
          'ext_profile': row[5],
          'profile_rate_ds': row[3],
          'profile_rate_us': row[4],
          'attainable_rate_ds': row[9],
          'attainable_rate_us': row[8],
          'snr_margin_ds': row[7],
          'snr_margin_us': row[6],
          'rate_ds': row[10],
          'rate_us': row[11],
          'inp_ds': row[12],
          'inp_us': row[13],
          'ln_atten_ds': row[14],
          'ln_atten_us': row[15],
        }

    # MA5600T VDSL
    query = """
  select v.dslam_id,v.port_id,vopr_ds.name,vopr_us.name,vops.name,vopi.name,vopsp.name,
  v.modulation,v.status,v.ln_atten_ds,v.ln_atten_us,v.kl0_co,v.kl0_cpe,v.snr_margin_ds,v.snr_margin_us,
  v.act_atp_ds,v.act_atp_us,v.ra_mode_ds,v.ra_mode_us,v.attainable_rate_ds,v.attainable_rate_us,
  v.act_data_rate_ds,v.act_data_rate_us,v.inp_ds,v.inp_us
  from vdsl_stats v left join vop_rate vopr_ds on vopr_ds.id=v.data_rate_profile_ds_id 
  left join vop_rate vopr_us on vopr_us.id=v.data_rate_profile_us_id 
  left join vop_snr vops on vops.id=v.noise_margin_profile_id
  left join vop_inp vopi on vopi.id=v.inp_profile_id
  left join vop_spectrum vopsp on vopsp.id=v.line_spectrum_profile_id
  where time > now() - interval(30) minute"""
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        dslam_id = row[0]
        port_id = row[1]
        dslamportkey = (dslam_id<<14) + port_id
        alluserstats[dslamportkey] = {}
        alluserstats[dslamportkey]['vop_rate_ds'] = row[2]
        alluserstats[dslamportkey]['vop_rate_us'] = row[3]
        alluserstats[dslamportkey]['vop_snr'] = row[4]
        alluserstats[dslamportkey]['vop_inp'] = row[5]
        alluserstats[dslamportkey]['vop_spectrum'] = row[6]
        alluserstats[dslamportkey]['modulation'] = row[7]
        alluserstats[dslamportkey]['ln_atten_ds'] = row[9]
        alluserstats[dslamportkey]['ln_atten_us'] = row[10]
        alluserstats[dslamportkey]['snr_margin_ds'] = row[13]
        alluserstats[dslamportkey]['snr_margin_us'] = row[14]
        alluserstats[dslamportkey]['attainable_rate_ds'] = row[19]
        alluserstats[dslamportkey]['attainable_rate_us'] = row[20]
        alluserstats[dslamportkey]['rate_ds'] = row[21]
        alluserstats[dslamportkey]['rate_us'] = row[22]
        alluserstats[dslamportkey]['inp_ds'] = row[23]
        alluserstats[dslamportkey]['inp_us'] = row[24]

    # MA5600T ADSL
    query = """
  select v.dslam_id,v.port_id,vopr_ds.name,vopr_us.name,vops.name,vopi.name,vopsp.name,
  v.status,v.ln_atten_ds,v.ln_atten_us,v.snr_margin_ds,v.snr_margin_us,
  v.act_atp_ds,v.act_atp_us,v.attainable_rate_ds,v.attainable_rate_us,
  v.act_data_rate_ds,v.act_data_rate_us,v.inp_ds,v.inp_us
  from adsl_stats v left join vop_rate vopr_ds on vopr_ds.id=v.data_rate_profile_ds_id 
  left join vop_rate vopr_us on vopr_us.id=v.data_rate_profile_us_id 
  left join vop_snr vops on vops.id=v.noise_margin_profile_id
  left join vop_inp vopi on vopi.id=v.inp_profile_id
  left join vop_spectrum vopsp on vopsp.id=v.line_spectrum_profile_id
  where time > now() - interval(30) minute"""
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        dslam_id = row[0]
        port_id = row[1]
        dslamportkey = (dslam_id<<14) + port_id
        alluserstats[dslamportkey] = {}
        alluserstats[dslamportkey]['vop_rate_ds'] = row[2]
        alluserstats[dslamportkey]['vop_rate_us'] = row[3]
        alluserstats[dslamportkey]['vop_snr'] = row[4]
        alluserstats[dslamportkey]['vop_inp'] = row[5]
        alluserstats[dslamportkey]['vop_spectrum'] = row[6]
        alluserstats[dslamportkey]['ln_atten_ds'] = row[8]
        alluserstats[dslamportkey]['ln_atten_us'] = row[9]
        alluserstats[dslamportkey]['snr_margin_ds'] = row[10]
        alluserstats[dslamportkey]['snr_margin_us'] = row[11]
        alluserstats[dslamportkey]['attainable_rate_ds'] = row[14]
        alluserstats[dslamportkey]['attainable_rate_us'] = row[15]
        alluserstats[dslamportkey]['rate_ds'] = row[16]
        alluserstats[dslamportkey]['rate_us'] = row[17]
        alluserstats[dslamportkey]['inp_ds'] = row[18]
        alluserstats[dslamportkey]['inp_us'] = row[19]



def getPortStatus():
    query = """select dslam_id,port_id,max_d_rate,max_u_rate,att_ds,att_us,es_ds,
            es_us,lols from dslam_ports_status where time > now() - interval '1' day"""
    cursor1.execute(query)
    rows = cursor1.fetchall()
    for row in rows:
        dslam_id = row[0]
        port_id = row[1]
        dslamportkey = (dslam_id<<14) + port_id
        portsstatus[dslamportkey] = {
            "max_d_rate": row[2],
            "max_u_rate": row[3],
            "att_ds": row[4],
            "att_us": row[5],
            "es_ds": row[6],
            "es_us": row[7],
            "lols": row[8]
        }





class Ma5600T(object):

    adslBoardTypes = ['H808ADPM', 'H802ADQD']
    vdslBoardTypes = ['H80BVDPM', 'H80AVDPD']

    def __init__(self):
        self.logger = logging.getLogger('MA5600T')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s")
        fh = logging.FileHandler("/var/log/apps/dslam_line_manager/dsl_line_manager.log")
        fh.setFormatter(formatter)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(logging.WARNING)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        self.vop_rate = {}
        self.vop_rate_inv = {}
        self.vop_spectrum = {}
        self.vop_spectrum_inv = {}
        self.vop_inp = {}
        self.vop_inp_inv = {}
        self.vop_snr = {}
        self.vop_snr_inv = {}
        # Rate profiles
        query = """select id, name, max_data_rate from vop_rate"""
        cursor1.execute(query)
        rows = cursor1.fetchall()
        for row in rows:
            self.vop_rate[row[0]] = {
                'name': row[1],
                'rate': row[2]
            }
            self.vop_rate_inv[row[1]] = {
                'id': row[0],
                'rate': row[2]
            }
        # Spectrum profiles
        query = """select id, name from vop_spectrum"""
        cursor1.execute(query)
        rows = cursor1.fetchall()
        for row in rows:
            self.vop_spectrum[row[0]] = {
                'name': row[1]
            }
            self.vop_spectrum_inv[row[1]] = {
                'id': row[0],
            }
        # INP profiles
        query = """select id, name, min_inp_shine_ds, min_inp_shine_us, max_delay_ds, max_delay_us from vop_inp"""
        cursor1.execute(query)
        rows = cursor1.fetchall()
        for row in rows:
            self.vop_inp[row[0]] = {
                'name': row[1],
                'inp_ds': row[2],
                'inp_us': row[3],
                'delay_ds': row[4],
                'delay_us': row[5]
            }
            self.vop_inp_inv[row[1]] = {
                'id': row[0],
                'inp_ds': row[2],
                'inp_us': row[3],
                'delay_ds': row[4],
                'delay_us': row[5]
            }
        # SNR profiles
        query = """select id,name,target_snr_ds,target_snr_us,max_snr_ds,max_snr_us from vop_snr"""
        cursor1.execute(query)
        rows = cursor1.fetchall()
        for row in rows:
            self.vop_snr[row[0]] = {
                'name': row[1],
                'target_snr': row[2],
                'max_snr': row[4]
            }
            self.vop_snr_inv[row[1]] = {
                'id': row[0],
                'target_snr': row[2],
                'max_snr': row[4]
            }

    def getPayed(self,dslamportkey):
        dslam_id = dslamportkey >> 14
        port_id = dslamportkey % (dslam_id<<14)
        if port_id < 1217:
            porttype = 'adsl'
        else:
            porttype = 'vdsl'
        servicename = userports[dslamportkey]['servicename']
        dataspeedcode =userports[dslamportkey]['dataspeedcode']
        voicespeedcode = userports[dslamportkey]['voicespeedcode']
        addstbs = userports[dslamportkey]['addstbs']
        opthd = userports[dslamportkey]['opthd']
        key = "%s:%s:%s"%(servicename,dataspeedcode,voicespeedcode)
        try:
            services[key]
        except KeyError:
            key = "%s:%s:None"%(servicename,dataspeedcode)
            if key not in services:
                self.logger.warning("""Unknown service for %s %s servicename=%s dataspeedcode=%s voicespeedcode=%s"""
                    %(dslams[dslam_id]['ime'],ports[port_id]['name'],servicename,dataspeedcode,voicespeedcode))
                return None
        iptv = services[key]['iptv']
        data = services[key]['data']
        voip = services[key]['voip']
        vdsl = services[key]['vdsl']
        traffic_ds = services[key]['traffic_ds']
        traffic_us = services[key]['traffic_us']
        payed_profile_ds = services[key]['profile_ds']
        payed_profile_us = services[key]['profile_us']
        if porttype == 'adsl':
            payed_profile_rate_ds = services[key]['rate_ds']
            payed_profile_rate_us = services[key]['rate_us']
            min_profile_rate_ds = services[key]['min_rate_ds']
            min_profile_rate_us = services[key]['min_rate_us']
            if addstbs:
                min_profile_rate_ds += addstbs*3600
                payed_profile_rate_ds += 3600 * addstbs
                payed_profile_ds = str(payed_profile_rate_ds / 1200) + "M"
            if opthd:
                payed_profile_rate_ds += 2400
                payed_profile_ds = str(payed_profile_rate_ds / 1200) + "M" 
            if payed_profile_rate_ds > 16800:
                payed_profile_rate_ds = 16800
                payed_profile_ds = "14M"
            if payed_profile_ds != "512K":
                payed_profile_ds += '-ATM'
        elif porttype == 'vdsl':
            payed_profile_rate_ds = services[key]['rate_ptm_ds']
            payed_profile_rate_us = services[key]['rate_ptm_us']
            min_profile_rate_ds = services[key]['min_rate_ptm_ds']
            min_profile_rate_us = services[key]['min_rate_ptm_us']
            if addstbs:
                min_profile_rate_ds += 3072*addstbs
                payed_profile_rate_ds += 3072*addstbs
                payed_profile_ds = str(payed_profile_rate_ds / 1024) + "M"
            if opthd:
                payed_profile_rate_ds += 2048*(addstbs+1)
                payed_profile_ds = str(payed_profile_rate_ds / 1024) + "M" 
            if vdsl == 0:
                if payed_profile_ds != "512K":
                    payed_profile_ds += "-ATM"
        payed = {
            'payed_profile_ds': payed_profile_ds,
            'payed_profile_us': payed_profile_us,
            'payed_profile_rate_ds': payed_profile_rate_ds,
            'payed_profile_rate_us': payed_profile_rate_us,
            'min_profile_rate_ds': min_profile_rate_ds,
            'min_profile_rate_us': min_profile_rate_us,
            'iptv': iptv,
            'data': data,
            'voip': voip,
            'vdsl': vdsl,
            'traffic_ds': traffic_ds,
            'traffic_us': traffic_us
        }
        return payed

    def run(self,dslamportkey):
        dslam_id = dslamportkey >> 14
        port_id = dslamportkey % (dslam_id<<14)
        status = {}
        status['result'] = {}
        status['error'] = {}
        payed = self.getPayed(dslamportkey)
        if payed is None:
            self.logger.warning("Unknown payed data for %s %s: %s"%(dslams[dslam_id]['ime'], ports[port_id]['name'],userports[dslamportkey]))
            return
        payed_profile_ds = payed['payed_profile_ds']
        payed_profile_us = payed['payed_profile_us']
        payed_profile_rate_ds = payed['payed_profile_rate_ds']
        payed_profile_rate_us = payed['payed_profile_rate_us']
        min_profile_rate_ds = payed['min_profile_rate_ds']
        min_profile_rate_us = payed['min_profile_rate_us']
        iptv = payed['iptv']
        data = payed['data']
        voip = payed['voip']
        vdsl = payed['vdsl']
        try:
            portsstatus[dslamportkey]
        except KeyError:
            self.logger.debug("%s %s: No port status information"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
            return status
        es_ds = portsstatus[dslamportkey]['es_ds']
        es_us = portsstatus[dslamportkey]['es_us']
        lols = portsstatus[dslamportkey]['lols']
        current_vop_rate_ds = alluserstats[dslamportkey]['vop_rate_ds']
        current_vop_rate_us = alluserstats[dslamportkey]['vop_rate_us']
        current_vop_snr = alluserstats[dslamportkey]['vop_snr']
        current_vop_inp = alluserstats[dslamportkey]['vop_inp']
        current_vop_spectrum = alluserstats[dslamportkey]['vop_spectrum']
        attainable_rate_ds = portsstatus[dslamportkey]['max_d_rate']
        attainable_rate_us = portsstatus[dslamportkey]['max_u_rate']
        current_rate_ds = alluserstats[dslamportkey]['rate_ds']
        current_rate_us = alluserstats[dslamportkey]['rate_us']
        snr_ds = alluserstats[dslamportkey]['snr_margin_ds'] 
        snr_us = alluserstats[dslamportkey]['snr_margin_us'] 
        inp_ds = alluserstats[dslamportkey]['inp_ds']
        inp_us = alluserstats[dslamportkey]['inp_us']
        current_profile_rate_ds = int(self.vop_rate_inv[current_vop_rate_ds]['rate'])
        current_profile_rate_us = int(self.vop_rate_inv[current_vop_rate_us]['rate'])

        # Profile check
        # Ds rate
        if current_profile_rate_ds > payed_profile_rate_ds:
            self.logger.debug("%s %s: To big ds profile payed=%s curr_prof_rate=%s"%(dslams[dslam_id]['ime'], ports[port_id]['name'], payed, current_profile_rate_ds))
            status['result']['profile_ds'] = payed_profile_ds
            status['result']['profile_ds_id'] = self.vop_rate_inv[payed_profile_ds]['id']
        elif current_profile_rate_ds < payed_profile_rate_ds:
            if iptv:
                if es_ds == 0 and lols == 0:
                    status['result']['profile_ds'] = payed_profile_ds
                    status['result']['profile_ds_id'] = self.vop_rate_inv[payed_profile_ds]['id']
                    self.logger.debug("%s %s: Increase ds profile"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
                else:
                    self.logger.debug("%s %s: Low profile, error line. lols=%s, es_ds=%s"%(dslams[dslam_id]['ime'], ports[port_id]['name'], lols, es_ds)) 
                    status['error']['ds'] = 5    # ne povecavaj zbog error statusa
            else:
                if lols < 2 and es_ds < 2:
                    status['result']['profile_ds'] = payed_profile_ds  
                    status['result']['profile_ds_id'] = self.vop_rate_inv[payed_profile_ds]['id']
                    self.logger.debug("%s %s: Increase ds profile"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
                else:
                    self.logger.debug("%s %s: Low ds profile, error line. lols=%s, es_ds=%s"%(dslams[dslam_id]['ime'], ports[port_id]['name'], lols, es_ds))
                    status['error']['ds'] = 5    # ne povecavaj zbog error statusa
        # US Profile check
        if current_profile_rate_us > payed_profile_rate_us:
            self.logger.debug("%s %s: To big us profile payed=%s curr_prof_rate=%s",dslams[dslam_id]['ime'], ports[port_id]['name'], payed, current_profile_rate_ds)
            status['result']['profile_us'] = payed_profile_us
        elif current_profile_rate_us < payed_profile_rate_us:
            if es_us < 3 and lols < 2:
                status['result']['profile_us'] = payed_profile_us
                status['result']['profile_us_id'] = self.vop_rate_inv[payed_profile_us]['id']
                self.logger.debug("%s %s: Increase us profile", dslams[dslam_id]['ime'], ports[port_id]['name'])
            else:
                self.logger.debug("%s %s: Low us profile. Lols=%s, es_us=%s", dslams[dslam_id]['ime'], ports[port_id]['name'], lols, es_us)
        # SNR + INP: djelujemo na ES
#        if not vdsl:
            # razlicito tretiramo adsl i vdsl korisnike
#            if iptv and es_ds > 1:
#                if inp_ds < 2:
#                    if current_vop_inp[] == "INP 2":
#                        inp_delay = "16/8"
#                elif inp_ds < 4:
#                    inp = 4
#                elif inp_ds < 8:
#                    inp = 8 
        target_snr = "8/8"
        if current_vop_snr[:3] == "8/8" and lols <= 1 and es_ds < 2:
            self.logger.debug("%s %s: Put target SNR 6/6 lols=%s es_ds=%s", dslams[dslam_id]['ime'], ports[port_id]['name'], lols, es_ds)
            target_snr = "6/6"
        elif current_vop_snr[:3] == "6/6":
            if lols > 2:
                self.logger.debug("%s %s: Increase target SNR, lols=%s",dslams[dslam_id]['ime'], ports[port_id]['name'],lols)
                target_snr = "8/8"
            elif es_ds > 2:
                self.logger.debug("%s %s: Increase target SNR, es_ds=%s",dslams[dslam_id]['ime'], ports[port_id]['name'],es_ds)
                target_snr = "8/8"
        # drugo odredimo max marginu
        max_snr = "31/31"
        if current_vop_snr[-5:] == "31/31":
            if payed_profile_rate_ds < 6144 and not iptv and lols < 2 and es_ds < 2:
                max_snr = "16/16"
            elif es_ds < 2 and es_us < 2 and lols < 2 and (snr_ds > 16 or snr_us > 16):
                max_snr = "16/16"
                self.logger.debug("%s %s: Put max SNR 16/16", dslams[dslam_id]['ime'], ports[port_id]['name'])
        elif current_vop_snr[-5:] == "16/16":
            if lols > 2:
                max_snr = "31/31"
            elif iptv and es_ds > 1:
                self.logger.debug("%s %s: Increase max SNR", dslams[dslam_id]['ime'], ports[port_id]['name'])
                max_snr = "31/31"
            elif es_ds > 2:
                self.logger.debug("%s %s: Increase max SNR", dslams[dslam_id]['ime'], ports[port_id]['name'])
                max_snr = "31/31"
        proposed_snr = target_snr + " " + max_snr
        if proposed_snr != current_vop_snr:
            status['result']['vop_snr'] = proposed_snr 
            status['result']['vop_snr_id'] = self.vop_snr_inv[proposed_snr]['id']
            self.logger.debug("%s %s: Setting vop_snr %s, prev snr %s"%(dslams[dslam_id]['ime'],ports[port_id]['name'],proposed_snr,current_vop_snr))
        # Spectrum
        current_vop_spectrum_id = self.vop_spectrum_inv[current_vop_spectrum]['id']
        if vdsl and alluserstats[dslamportkey]['modulation'] != 5:
            # posalji to u event handler??
            self.logger.info("%s %s: VDSL not active"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
        elif vdsl and current_vop_spectrum != 'ADSL':
            # nemamo neko pravilo za 17a ili 8b profile zasada
            # zelimo postaviti annex M, i zato radimo ovaj hack dolje (na parnim indeksima su annex b, na neparnim annex m)
            if current_vop_spectrum_id % 2 == 0:
                status['result']['vop_spectrum_id'] = current_vop_spectrum_id + 1
                status['result']['vop_spectrum'] = self.vop_spectrum[current_vop_spectrum_id + 1]
                self.logger.debug("%s %s: Setting vop_spectrum %s"%(dslams[dslam_id]['ime'], ports[port_id]['name'],status['result']['vop_spectrum']))
        elif vdsl and current_vop_spectrum == 'ADSL':
            self.logger.warning("%s %s: VDSL korisnik s adsl profilom"%(dslams[dslam_id]['ime'], ports[port_id]['name'])) 
            status['result']['vop_spectrum_id'] = 301
            status['result']['vop_spectrum'] = self.vop_spectrum[301]

#        if payed['traffic_ds'] != getTrafficData(dslam_id, port_id, vlan=86, vpi=0, vci=86)
        return status



class Ma5600(object):

    adslBoardTypes = ['H561ADBF','H565ADBF','H565ADBF2','H565ADBF3'] 

    def __init__(self):
        self.logger = logging.getLogger('MA5600')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s")
        fh = logging.FileHandler("/var/log/apps/dslam_line_manager/dsl_line_manager.log")
        fh.setFormatter(formatter)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(logging.WARNING)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        self.profiles = {}
        self.profiles_inv = {}
        self.ext_profiles = {}
        query = """select line_profile_index,name,max_dl,max_upl from profili where visable = 1"""
        cursor1.execute(query)
        rows = cursor1.fetchall()
        for row in rows:
            self.profiles[row[0]] = {
                'name': row[1],
                'rate_ds': row[2],
                'rate_us': row[3],
            }
            self.profiles_inv[row[1]] = {
                'id': row[0],
                'rate_ds': row[2],
                'rate_us': row[3],
            }
        query = """select extended_profile_index,extended_profile_name from extended_profile where visable = 1"""
        cursor1.execute(query)
        rows = cursor1.fetchall()
        for row in rows:
            self.ext_profiles[row[0]] = {
                'name': row[1],
            }
 
    def getPayed(self,dslamportkey,margin="9/9", channel="INTER", adaptive="S"):
        dslam_id = dslamportkey >> 14
        port_id = dslamportkey % (dslam_id<<14)
        servicename = userports[dslamportkey]['servicename']
        dataspeedcode =userports[dslamportkey]['dataspeedcode']
        voicespeedcode = userports[dslamportkey]['voicespeedcode']
        addstbs = userports[dslamportkey]['addstbs']
        opthd = userports[dslamportkey]['opthd']
        key = "%s:%s:%s"%(servicename,dataspeedcode,voicespeedcode)
        try:
            services[key]
        except KeyError:
            logger.warning("""Unknown service for %s %s servicename=%s dataspeedcode=%s voicespeedcode=%s"""
                %(dslams[dslam_id]['ime'],adslports[port_id]['name'],servicename,dataspeedcode,voicespeedcode))
            return None
        iptv = services[key]['iptv']
        data = services[key]['data']
        voip = services[key]['voip']
        payed_profile_ds = services[key]['profile_ds']
        payed_profile_us = services[key]['profile_us']
        payed_profile_rate_ds = services[key]['rate_ds']
        payed_profile_rate_us = services[key]['rate_us']
        min_profile_rate_ds = services[key]['min_rate_ds']
        min_profile_rate_us = services[key]['min_rate_us']
        if addstbs:
            min_profile_rate_ds += addstbs*3600
            payed_profile_rate_ds += 3600 * addstbs
            payed_profile_ds = str(payed_profile_rate_ds / 1200) + "M"
        if opthd:
            payed_profile_rate_ds += 2400
            payed_profile_ds = str(payed_profile_rate_ds / 1200) + "M" 
        if payed_profile_rate_ds > 16800:
            payed_profile_rate_ds = 16800
            payed_profile_ds = "14M"
        if not iptv and payed_profile_rate_ds < 6000:
            if payed_profile_rate_ds < 1000:
                payed_profile_ds = re.search("(\d+)",payed_profile_ds).group(1)
                profile_name = "%s/%s 9/9(12/12) I R"%(payed_profile_ds, payed_profile_rate_us)
            else:
                profile_name = "%s/%s 9/9(12/12) I R"%(payed_profile_ds, payed_profile_rate_us)
        else:
            profile_name = "%s/%s %s %s %s"%(payed_profile_ds,payed_profile_rate_us,margin,channel,adaptive)
        profile_id = self.profiles_inv[profile_name]['id']
        payed = {
            'profile_id': profile_id,
            'payed_profile_ds': payed_profile_ds,
            'payed_profile_us': payed_profile_us,
            'payed_profile_rate_ds': payed_profile_rate_ds,
            'payed_profile_rate_us': payed_profile_rate_us,
            'min_profile_rate_ds': min_profile_rate_ds,
            'min_profile_rate_us': min_profile_rate_us,
            'iptv': iptv,
            'data': data,
            'voip': voip
        }
        return payed


    def run(self,dslamportkey):
        dslam_id = dslamportkey >> 14
        port_id = dslamportkey % (dslam_id<<14)
        status = {}
        status['result'] = {}
        status['error'] = {}
        payed = self.getPayed(dslamportkey)
        if payed is None:
            self.logger.warning("%s %s: Unknown payed data - %s"%(dslams[dslam_id]['ime'], ports[port_id]['name'],userports[dslamportkey]))
            return
        payed_profile_id = payed['profile_id']
        payed_profile_ds = payed['payed_profile_ds']
        payed_profile_us = payed['payed_profile_us']
        payed_profile_rate_ds = payed['payed_profile_rate_ds']
        payed_profile_rate_us = payed['payed_profile_rate_us']
        min_profile_rate_ds = payed['min_profile_rate_ds']
        min_profile_rate_us = payed['min_profile_rate_us']
        iptv = payed['iptv']
        data = payed['data']
        voip = payed['voip']
        current_profile_id = alluserstats[dslamportkey]['line_profile']
        current_ext_id = alluserstats[dslamportkey]['ext_profile']
        try:
            portsstatus[dslamportkey]
        except KeyError:
            self.logger.debug("%s %s: No port status information"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
            return status
        es_ds = portsstatus[dslamportkey]['es_ds']
        es_us = portsstatus[dslamportkey]['es_us']
        lols = portsstatus[dslamportkey]['lols']
        attainable_rate_ds = portsstatus[dslamportkey]['max_d_rate']
        attainable_rate_us = portsstatus[dslamportkey]['max_u_rate']
        current_rate_ds = alluserstats[dslamportkey]['rate_ds']
        current_rate_us = alluserstats[dslamportkey]['rate_us']
        inp_ds = alluserstats[dslamportkey]['inp_ds']
        inp_us = alluserstats[dslamportkey]['inp_us']
#        try:
#            current_profile_rate_ds = self.profiles[payed_profile_id]['rate_ds']
#            current_profile_rate_us = self.profiles[payed_profile_id]['rate_us']
#        except KeyError:
#            logger.info("Unknown profile on %s %s"%(dslams[dslam_id]['ime'], adslports[port_id]['name']))
#            status['result']['new_profile_id'] = payed_profile_id
#            return status

        # Profile check
        if current_profile_id >= 990:
            # ove necu dirati
            return None

        if current_profile_id not in self.profiles:
            self.logger.info("%s %s: Unknown profile"%(dslams[dslam_id]['ime'], adslports[port_id]['name']))
            status['result']['new_profile_id'] = payed_profile_id
            status['change'] = 1
            return status
        else:
            current_profile_rate_ds = self.profiles[current_profile_id]['rate_ds']
            current_profile_rate_us = self.profiles[current_profile_id]['rate_us']

        # Ds profile check
        if current_profile_rate_ds > payed_profile_rate_ds:
            self.logger.debug("%s %s: To big ds profile"%(dslams[dslam_id]['ime'], adslports[port_id]['name']))
            status['result']['profile_ds'] = payed_profile_ds
        elif current_profile_rate_ds < payed_profile_rate_ds:
            if iptv:
                if es_ds == 0 and lols == 0:
                    status['result']['profile_ds'] = payed_profile_ds
                    self.logger.debug("%s %s: Increase ds profile"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
#                elif es_ds < 2 and lols < 2:
#                    status['result']['new_profile_id'] = payed_profile_id % 100 + current_profile_id / 100 * 100 
#                    logger.debug("!Increase profile on %s %s"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
#                elif es_ds == 1 and lols == 0:
#                    status['result']['new_profile_id'] = payed_profile_id % 100 + current_profile_id / 100 * 100 
#                    logger.debug("Increase profile on %s %s"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
#                elif es_ds == 0 and lols == 1:
#                    status['result']['new_profile_id'] = payed_profile_id % 100 + current_profile_id / 100 * 100
#                    logger.debug("Increase profile on %s %s"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
                else:
                    self.logger.debug("%s %s: Low profile, error line"%(dslams[dslam_id]['ime'], ports[port_id]['name'])) 
                    status['error']['ds'] = 5    # ne povecavaj zbog error statusa
            else:
                if lols < 2 and es_ds < 2:
                    if payed_profile_id != 661:
                        status['result']['profile_ds'] = payed_profile_ds  
                        self.logger.debug("%s %s: Increase ds profile"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
#                elif lols < 2 and es_ds < 3 and es_us < 3:
#                    status['result']['new_profile_id'] = payed_profile_id % 100 + current_profile_id / 100 * 100 
#                    logger.debug("!Increase profile on %s %s"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
                else:
                    self.logger.debug("%s %s: Low profile, error line"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
                    status['error']['ds'] = 5    # ne povecavaj zbog error statusa
        elif es_ds == 0:
            if current_ext_id > 0:
                self.logger.debug("%s %s: Remove extended_id",dslams[dslam_id]['ime'], ports[port_id]['name'])
                status['result']['new_extended_id'] = None
            if current_profile_id >= 400 and current_profile_id < 500:
                self.logger.debug("%s %s: Remove I+",dslams[dslam_id]['ime'], ports[port_id]['name'])
                status['result']['interleaved'] = 0

        # Us  profile check
        if current_profile_rate_us > payed_profile_rate_us:
            self.logger.debug("%s %s: To big us profile in us"%(dslams[dslam_id]['ime'], adslports[port_id]['name']))
            profile_us = payed_profile_rate_us
            if profile_us > 1024:
                self.logger.error("%s %s: Invalid profile_us name %s"%(dslams[dslam_id]['ime'],ports[port_id]['name'],profile_us))
                return None
            else:
                profile_us = str(profile_us)
        elif current_profile_rate_us < payed_profile_rate_us:
            if es_us < 2 and lols < 2:
                profile_us = payed_profile_rate_us
                self.logger.debug("%s %s: Increase us profile"%(dslams[dslam_id]['ime'], ports[port_id]['name']))
                if profile_us > 1024:
                    self.logger.error("%s %s: Invalid profile_us name %s"%(dslams[dslam_id]['ime'],ports[port_id]['name'], profile_us))
                    return None
                profile_us = str(profile_us)
            else:
                self.logger.debug("%s %s: Low us profile, error line. lols=%s, es_us=%s"%(dslams[dslam_id]['ime'], ports[port_id]['name'], lols, es_us)) 
                status['error']['us'] = 5    # ne povecavaj zbog error statusa
                profile_us = current_profile_rate_us
                if profile_us > 1024:
                    self.logger.error("%s %s: Invalid profile_us name %s"%(dslams[dslam_id]['ime'],ports[port_id]['name'],profile_us))
                    return None
                profile_us = str(profile_us)
        else:
            profile_us = current_profile_rate_us
            if profile_us > 1024:
                self.logger.error("%s %s: Invalid profile_us name %s"%(dslams[dslam_id]['ime'],ports[port_id]['name'], profile_us))
            else:
                profile_us = str(profile_us)
        status['result']['profile_us'] = profile_us


        # if we are in next if, it could mean 
        # a) we decrease line ds line rate so we don't care about errors 
        # b) we increase line ds rate, but there are no errors
        # if errors in upstream, upstream rate is not increased
#        if 'profile_ds' in status['result']:
#            if payed_profile_rate_ds <= 6000 and not iptv:
#                profile_name = "%s/%s 9/9(12/12) I R"%(payed_profile_ds, payed_profile_rate_us)
#                status['result']['new_profile_id'] = self.profiles_inv[profile_name]['id']
#            else:
#                profile_name = "%s/%s %s INTER S"%(status['result']['profile_ds'],profile_us,"9/9")
#                new_profile_id = self.profiles_inv[profile_name]['id']
#                status['result']['new_profile_id'] = (new_profile_id % 100) + current_profile_id - current_profile_id%100 
#            logger.debug("%s %s: new profile %s", dslams[dslam_id]['ime'], ports[port_id]['name'], profile_name)
#            return status


        # Ds error checks. 
        if iptv:
#            if 600 <= current_profile_id <= 700:
#                status['result']['new_profile_id'] = payed_profile_id
            if es_ds >= 3:
                status['es_ds_action'] = 1
                if 500 <= current_profile_id < 599:
                    self.logger.debug("%s %s: FAST profile",dslams[dslam_id]['ime'], ports[port_id]['name'])
                    status['new_profile_id'] = payed_profile_id
                elif 600 <= current_profile_id <= 699:
                    self.logger.debug("%s %s: 12/12 profile, errored iptv port",dslams[dslam_id]['ime'], ports[port_id]['name'])
                    status['result']['new_profile_id'] = (payed_profile_id % 100) + 400
                    status['result']['interleaved'] = 1
                elif inp_ds > 4:
                    # inp already big enough
                    if 400 <= current_profile_id < 500:
                        self.logger.debug("%s %s: Maximum protection, errors present",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['error']['ds'] = 1
                    else:
                        self.logger.debug("%s %s: Put I+",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                        status['result']['new_profile_id'] = (current_profile_id % 100) + 400
                        status['result']['interleaved'] = 1
                elif getBoardType(dslam_id,port_id) == 'H561ADBF':
                    # centilium board, it's not functioning
                    if 400 <= current_profile_id < 500:
                        self.logger.debug("%s %s: Maximum protection,centilium",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['error']['ds'] = 3
                    else:
                        self.logger.debug("%s %s: Put I+, centilium",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                        status['result']['new_profile_id'] = (current_profile_id % 100) + 400
                        status['result']['interleaved'] = 1
                else:
                    if payed_profile_rate_ds < attainable_rate_ds:
                        self.logger.debug("%s %s: Extended=2",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['result']['new_extended_id'] = 2
                    elif min_profile_rate_ds < current_rate_ds:
                        if current_profile_id < 400 or current_profile_id >= 500:
                            self.logger.debug("%s %s: Put I+",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                            status['result']['new_profile_id'] = (current_profile_id % 100) + 400
                            status['result']['interleaved'] = 1
                        elif inp_ds < 2:
                            self.logger.debug("%s %s: Extended=1",dslams[dslam_id]['ime'], ports[port_id]['name'])
                            status['result']['new_extended_id'] = 1
                        elif inp_ds < 4:
                            self.logger.debug("%s %s: Extended=2",dslams[dslam_id]['ime'], ports[port_id]['name'])
                            status['result']['new_extended_id'] = 2
                        else:
                            self.logger.debug("%s %s: Max protection,low rate",dslams[dslam_id]['ime'], ports[port_id]['name'])
                            status['error']['ds'] = 2
                    else:
                        # ds_rate to low, can't put inp
                        self.logger.debug("%s %s: Low rate, can't do much",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['error']['ds'] = 2
            elif es_ds == 2:
                status['es_ds_action'] = 1
                if 500 <= current_profile_id < 599:
                    self.logger.debug("%s %s: FAST profile",dslams[dslam_id]['ime'], ports[port_id]['name'])
                    status['new_profile_id'] = payed_profile_id
                elif 600 <= current_profile_id <= 699:
                    self.logger.debug("%s %s: 12/12 profile, errored iptv port",dslams[dslam_id]['ime'], ports[port_id]['name'])
                    status['new_profile_id'] = payed_profile_id 
                elif inp_ds > 4:
                    # inp already high
                    if 400 <= current_profile_id < 500:
                        self.logger.debug("%s %s: Maximum protection,errors present",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['error']['ds'] = 1
                    else:
                        self.logger.debug("%s %s: Put I+",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                        status['result']['new_profile_id'] = (current_profile_id % 100) + 400
                        status['result']['interleaved'] = 1
                elif getBoardType(dslam_id,port_id) == 'H561ADBF':
                    # centilium board, can't apply INP on centilium board
                    if 400 <= current_profile_id < 500:
                        self.logger.debug("%s %s: Maximum protection, centilium",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['error']['ds'] = 3
                    else:
                        self.logger.debug("%s %s: Put I+, centilium",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                        status['result']['new_profile_id'] = (current_profile_id % 100) + 400
                        status['result']['interleaved'] = 1
                else:
                    if current_profile_rate_ds < attainable_rate_ds*9/10 or payed_profile_rate_ds < current_rate_ds:
                        if inp_ds > 4:
                            if current_profile_id < 400 or current_profile_id >= 500:
                                self.logger.debug("%s %s: Put I+",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                                status['result']['new_profile_id'] = (current_profile_id % 100) + 400
                                status['result']['interleaved'] = 1
                            else:
                                status['error']['ds'] = 4    # max zastitni profil je vec stavljen
                        elif alluserstats[dslamportkey]['inp_ds'] > 2:
                            self.logger.debug("%s %s: Extended=2",dslams[dslam_id]['ime'], ports[port_id]['name'])
                            status['result']['new_extended_id'] = 2
                        else:
                            self.logger.debug("%s %s: Extended=1",dslams[dslam_id]['ime'], ports[port_id]['name'])
                            status['result']['new_extended_id'] = 1
                    else:
                        # ds_rate to low, can't put inp
                        self.logger.debug("%s %s: Low rate, can't do much",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['error']['ds'] = 2
        else: 
            if es_ds == 4:
                if payed_profile_id == 661 and current_profile_id != 661:
                    self.logger.debug("%s %s: Put SOLO profile",dslams[dslam_id]['ime'], ports[port_id]['name'])
                    status['new_profile_id'] = payed_profile_id  
                elif 500 <= current_profile_id < 599:
                    self.logger.debug("%s %s: FAST profile",dslams[dslam_id]['ime'], ports[port_id]['name'])
                    status['new_profile_id'] = payed_profile_id
                elif inp_ds > 4:
                    # inp already high
                    if 400 <= current_profile_id < 500:
                        self.logger.debug("%s %s: Maximum protection",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['error']['ds'] = 1
                    else:
                        self.logger.debug("%s %s: Put I+",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                        status['result']['new_profile_id'] = (current_profile_id % 100) + 400
                        status['result']['interleaved'] = 1
                elif getBoardType(dslam_id,port_id) == 'H561ADBF':
                    # centilium board, can't apply INP on centilium board
                    if 400 <= current_profile_id < 500:
                        self.logger.debug("%s %s: Maximum protection, centilium",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['error']['ds'] = 3
                    else:
                        self.logger.debug("%s %s: Put I+, centilium",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                        status['result']['new_profile_id'] = (current_profile_id % 100) + 400
                        status['result']['interleaved'] = 1
                else:
                    if 600 <= current_profile_id <= 700 and current_profile_id != 661:
                        self.logger.debug("%s %s: 12/12 profile",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                        status['result']['new_profile_id'] = (payed_profile_id % 100) + 400
                        status['result']['interleaved'] = 1
                    if current_profile_rate_ds > attainable_rate_ds or payed_profile_rate_ds < current_rate_ds:
                        self.logger.debug("%s %s: Extended=2",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['result']['new_extended_id'] = 2
                    else:
                        # ds_rate to low, can't put inp
                        self.logger.debug("%s %s: ds rate to low, can't do much",dslams[dslam_id]['ime'], ports[port_id]['name'])
                        status['error']['ds'] = 2

        if es_us == 4 and status['result'] == {}:
            if current_profile_id < 400 or current_profile_id >= 500:
                self.logger.debug("%s %s: Put I+",dslams[dslam_id]['ime'], ports[port_id]['name'])
#                status['result']['new_profile_id'] = (current_profile_id % 100) + 400
                status['result']['interleaved'] = 1
                status['change'] = 1
            else:
                self.logger.debug("%s %s: Maximum protection",dslams[dslam_id]['ime'], ports[port_id]['name'])
                status['error']['us'] = 4

        return status

#####################################################
# tu sad pronalazimo profil iz naziva za ds i us
####################################################
        if 'profile_us' in status['result'].keys():
            profile_us = status['result']['profile_us']
            profile_us = re.sub("(M|K)", '', profile_us)
            if profile_us == '1':
                profile_us = '1024'
            elif int(profile_us) <= 100:
                self.logger.error("%s %s: Unknown profile %s",dslams[dslam_id]['ime'], ports[port_id]['name'], status['result']['profile_us'])
        else:
            profile_us = current_profile_rate_us
            if profile_us > 1024:
                self.logger.error("%s %s: Invalid profile_us name %s",dslams[dslam_id]['ime'], ports[port_id]['name'], profile_us)
            else:
                profile_us = str(profile_us)

        if 'profile_ds' in status['result']:
            profile_ds = status['result']['profile_ds']
            if 'profile_us' not in status['result']:
                profile_us = payed_profile_us
                profile_us = re.sub("(M|K)", '', profile_us)
                if profile_us == '1':
                    profile_us = '1024'
                elif int(profile_us) <= 100:
                    self.logger.error("%s %s: Unknown profile_us %s status=%s",dslams[dslam_id]['ime'], ports[port_id]['name'], status['result']['profile_us'],status)
                    return None
        else:
            profile_ds = current_profile_rate_ds / 1200
            if profile_ds == 0:
                profile_ds = payed_profile_ds
                self.logger.error("%s %s: Invalid profile_ds name %s status=%s",dslams[dslam_id]['ime'], ports[port_id]['name'], profile_ds,status)
                return None
            else:
                profile_ds = str(profile_ds) + 'M'


        profile_name = "%s/%s %s INTER S"%(profile_ds,profile_us,"9/9")
        proposed_profile_id = self.profiles_inv[profile_name]['id']
        if 'new_profile_id' in status['result']:
            # posalji dslam_id, port_id, new_profile_id, extended_id
            status['result']['new_profile_id'] = (status['result']['new_profile_id'] % 100) + current_profile_id - current_profile_id%100 
            self.logger.info("%s %s: Change profile %s",dslams[dslam_id]['ime'], ports[port_id]['name'],status['result'])
        else:
            #izracunaj new_profile_id i posalji uz dslam_id, port_id i extended_id (ako postoji)
            status['result']['new_profile_id'] = (proposed_profile_id % 100) + current_profile_id - current_profile_id%100 
            self.logger.info("%s %s: Change profile %s",dslams[dslam_id]['ime'], ports[port_id]['name'],status['result'])

        if status['result'] != {} and status['error'] != {}:
            self.logger.error("%s %s: Nesto ne stima %s",dslams[dslam_id]['ime'], ports[port_id]['name'],status)
        elif status['result'] != {}:
            self.logger.debug("%s %s: RESULT %s",dslams[dslam_id]['ime'], ports[port_id]['name'],status['result'])
        elif status['error']:
            self.logger.debug("%s %s: ERROR %s",dslams[dslam_id]['ime'], ports[port_id]['name'],status['error'])

        return status



#dslamdb.dslamclose(conn1, cursor1)
#provdb.provclose(conn2, cursor2)


if __name__ == '__main__':
    ma5600 = Ma5600()
    ma5600t = Ma5600T()
    getDslams()              # dslams, dslams_inv
    getPorts()               # ports, ports_inv
    getProtected()           # protected
    getUsers()               # userports
    getAllUserCurrent()      # alluserstats
    getPortStatus()          # portsstatus
    bindServices2Profiles()
    for dslamportkey, userdata in userports.iteritems():
        dslam_id = dslamportkey >> 14
        port_id = dslamportkey % (dslam_id<<14)
        if dslams[dslam_id]['dslam_type_id'] == 1:
            status = ma5600.run(dslamportkey)
            if status is None:
                # ne treba nista raditi s tim portom
                continue
            elif 'result' in status:
                # line profile results
                profile = {}
                if 'new_profile_id' in status['result']:
                    profile_id = status['result']['new_profile_id']
                    profile['line_profile'] = ma5600.profiles[profile_id]['name']
                if 'new_extended_id' in status['result']:
                    extended_id = status['result']['new_extended_id']
                    profile['extended_profile'] = ma5600.ext_profiles[extended_id]['name']
                if 'change' in status:
#                    red.hset('set:profile:%s'%dslams[dslam_id]['ip'],ports[port_id]['name'], json.dumps(profile))
                    redpipe.hset('set:profile:%s'%dslams[dslam_id]['ip'],ports[port_id]['name'], json.dumps(profile))
                    redpipe.expire('set:profile:%s'%dslams[dslam_id]['ip'], 86400)

        elif dslams[dslam_id]['dslam_type_id'] in (2,3,4):
            status = ma5600t.run(dslamportkey)
            if status is None:
                continue
            elif 'result' in status:
                vop = {}
                if 'vop_spectrum' in status['result']:
                    vop['spectrum'] = status['result']['vop_spectrum_id']
                if 'profile_ds' in status['result']:
                    vop['ds_rate'] = status['result']['profile_ds_id']
                if vop.keys():
#                    red.hset("set:profile:%s"%dslams[dslam_id]['ip'],ports[port_id]['name'], json.dumps(vop))
                    redpipe.hset("set:profile:%s"%dslams[dslam_id]['ip'],ports[port_id]['name'], json.dumps(vop))
                    redpipe.expire("set:profile:%s"%dslams[dslam_id]['ip'], 86400)
    redpipe.execute()
#        elif dslams[dslam_id]['dslam_type_id'] in (2,3,4):
#            Ma5600T.run(dslamportkey)
