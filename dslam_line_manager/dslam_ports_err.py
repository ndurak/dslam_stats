#!/usr/bin/python

import datetime
import MySQLdb
import decimal
import threading
from ConfigParser import ConfigParser
import logging
import os


config = ConfigParser()
config.read('dsl_line_manager.conf')

logfile = config.get('Logging', 'logfile')
loglevel = config.get('Logging', 'loglevel')

logger = logging.getLogger("DslamPortError")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s")

# logging to the file
fh = logging.FileHandler(logfile)
fh.setFormatter(formatter)

# logging to the console
ch = logging.StreamHandler()
ch.setFormatter(formatter)

logger.addHandler(fh)

conn = MySQLdb.connect (host = "mysqldb.net",
                        user = "user",
                        passwd = "pw123",
                        db = "dslam")
conn.autocommit(True)
cursor = conn.cursor()

starttime = (datetime.datetime.now() - datetime.timedelta(hours=6)).replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
#endtime = datetime.datetime.now()      #.strftime("%Y-%m-%d %H:%M:%S")
#endtime = datetime.datetime.now().replace(minute=29, second=0)       #.strftime("%Y-%m-%d %H:%M:%S")

def getESThresholds6h(*args,**kwargs):
    global es_thresh_1,es_thresh_2,es_thresh_3,es_thresh_4
    global ses_thresh_1,ses_thresh_2,ses_thresh_3,ses_thresh_4
    if len(args) == 4:
        if args[0][0] < args[1][0] < args[2][0] < args[3][0]: 
            es_thresh_1 = args[0][0]
            es_thresh_2 = args[1][0]
            es_thresh_3 = args[2][0]
            es_thresh_4 = args[3][0]
            ses_thresh_1 = args[0][1]
            ses_thresh_2 = args[1][1]
            ses_thresh_3 = args[2][1]
            ses_thresh_4 = args[3][1]
        else:
            es_thresh_1 = int(config.get("ES thresholds 6h", "es_thresh_y"))
            ses_thresh_1 = int(config.get("ES thresholds 6h", "ses_thresh_y"))
            es_thresh_2 = int(config.get("ES thresholds 6h", "es_thresh_o"))
            ses_thresh_2 = int(config.get("ES thresholds 6h", "ses_thresh_o"))
            es_thresh_3 = int(config.get("ES thresholds 6h", "es_thresh_r"))
            ses_thresh_3 = int(config.get("ES thresholds 6h", "ses_thresh_r"))
            es_thresh_4 = int(config.get("ES thresholds 6h", "es_thresh_b"))
            ses_thresh_4 = int(config.get("ES thresholds 6h", "ses_thresh_b"))
    else:
        es_thresh_1 = int(config.get("ES thresholds 6h", "es_thresh_y"))
        ses_thresh_1 = int(config.get("ES thresholds 6h", "ses_thresh_y"))
        es_thresh_2 = int(config.get("ES thresholds 6h", "es_thresh_o"))
        ses_thresh_2 = int(config.get("ES thresholds 6h", "ses_thresh_o"))
        es_thresh_3 = int(config.get("ES thresholds 6h", "es_thresh_r"))
        ses_thresh_3 = int(config.get("ES thresholds 6h", "ses_thresh_r"))
        es_thresh_4 = int(config.get("ES thresholds 6h", "es_thresh_b"))
        ses_thresh_4 = int(config.get("ES thresholds 6h", "ses_thresh_b"))



class MA5600T(object):

    def adsl_ports_err(self):
        endtime = datetime.datetime.now().replace(minute=29, second=0)
        query = """ select time, dslam_id, port_id, data_rate_profile_ds_id, data_rate_profile_us_id, line_spectrum_profile_id, status, ln_atten_ds, ln_atten_us, snr_margin_ds, snr_margin_us, act_atp_ds, act_atp_us, attainable_rate_ds, attainable_rate_us, inp_ds, inp_us, es_ds, es_us, ses_ds, ses_us, init_times from adsl_stats where time between '%s' and '%s' and init_times > 0 order by dslam_id, port_id, time """%(starttime, endtime)

        cursor.execute(query)
        rows = cursor.fetchall()

        i = 0
        stats = {}
        status = {}
        insert_data = []
        endtime = endtime.replace(minute=0)
        for row in rows:
            dslam_id = row[1]
            port_id = row[2]
            dslamportkey = (dslam_id<<14) + port_id
            try:
               stats[dslamportkey]
            except KeyError:
                # first entry for this port
                i = 0
                stats[dslamportkey] = {}
                stats[dslamportkey]['es_ds'] = []
                stats[dslamportkey]['ses_ds'] = []
                stats[dslamportkey]['es_us'] = []
                stats[dslamportkey]['ses_us'] = []
                stats[dslamportkey]['inits'] = []
                es_ds_ref = row[17]
                es_us_ref = row[18]
                ses_ds_ref = row[19]
                ses_us_ref = row[20]
                inits_ref = row[21]
                continue
            es_ds = row[17]
            es_us = row[18]
            ses_ds = row[19]
            ses_us = row[20]
            inits = row[21]
            stats[dslamportkey]['es_ds'].append(es_ds - es_ds_ref)
            es_ds_ref = es_ds
            stats[dslamportkey]['ses_ds'].append(ses_ds - ses_ds_ref)
            ses_ds_ref = ses_ds
            stats[dslamportkey]['es_us'].append(es_us - es_us_ref)
            es_us_ref = es_us
            stats[dslamportkey]['ses_us'].append(ses_us - ses_us_ref)
            ses_us_ref = ses_us
            stats[dslamportkey]['inits'].append(inits - inits_ref)
            inits_ref = inits

        for dslamportkey, errors in stats.iteritems():
            es_ds = errors['es_ds']
            ses_ds = errors['ses_ds']
            es_us = errors['es_us']
            ses_us = errors['ses_us']
            inits = errors['inits']
            status[dslamportkey] = {'ds': {}, 'us': {}}
            dslam_id = dslamportkey >> 14
            port_id = dslamportkey % (dslam_id<<14)

            # 0 - white, 1 - orange, 2 - red, 3 - black
            status[dslamportkey]['ds'][0] = 0
            status[dslamportkey]['ds'][1] = 0
            status[dslamportkey]['ds'][2] = 0
            status[dslamportkey]['ds'][3] = 0
            status[dslamportkey]['ds'][4] = 0
            status[dslamportkey]['us'][0] = 0
            status[dslamportkey]['us'][1] = 0
            status[dslamportkey]['us'][2] = 0
            status[dslamportkey]['us'][3] = 0
            status[dslamportkey]['us'][4] = 0


            for i in range(len(es_ds)):
                if es_ds[i] < es_thresh_1 and ses_ds[i] < ses_thresh_1:
                    status[dslamportkey]['ds'][0] += 1
                elif es_ds[i] < es_thresh_2 and ses_ds[i] < ses_thresh_2:
                    status[dslamportkey]['ds'][1] += 1
                elif es_ds[i] < es_thresh_3 and ses_ds[i] < ses_thresh_3:
                    status[dslamportkey]['ds'][2] += 1
                elif es_ds[i] < es_thresh_4 and ses_ds[i] < ses_thresh_4:
                    status[dslamportkey]['ds'][3] += 1
                else:
                    status[dslamportkey]['ds'][4] += 1
            if len(es_ds) >= 8:
                es_ds_err_status = status[dslamportkey]['ds'][1] + status[dslamportkey]['ds'][2]*2 + status[dslamportkey]['ds'][3]*3 + status[dslamportkey]['ds'][4]*4
            else:
                es_ds_err_status = -1


            for i in range(len(es_us)):
                if es_us[i] < es_thresh_1 and ses_us[i] < ses_thresh_1:
                    status[dslamportkey]['us'][0] += 1
                elif es_us[i] < es_thresh_2 and ses_us[i] < ses_thresh_2:
                    status[dslamportkey]['us'][1] += 1
                elif es_us[i] < es_thresh_3 and ses_us[i] < ses_thresh_3:
                    status[dslamportkey]['us'][2] += 1
                elif es_us[i] < es_thresh_4 and ses_us[i] < ses_thresh_4:
                    status[dslamportkey]['us'][3] += 1
                else:
                    status[dslamportkey]['us'][4] += 1
            if len(es_us) >= 8:
                es_us_err_status = status[dslamportkey]['us'][1] + status[dslamportkey]['us'][2]*2 + status[dslamportkey]['us'][3]*3 + status[dslamportkey]['us'][4]*4
            else:
                es_us_err_status = -1
            insert_data.append((endtime,dslam_id, port_id, es_ds_err_status, es_us_err_status))

        query = """insert into dslam_ports_err (time, dslam_id, port_id, err_ds_status, err_us_status)
                values (%s, %s, %s, %s, %s)"""
        cursor.executemany(query, insert_data)


    def vdsl_ports_err(self):
        global starttime,endtime
        endtime = datetime.datetime.now().replace(minute=29, second=0)
        query = """ select time, dslam_id, port_id, data_rate_profile_ds_id, data_rate_profile_us_id, line_spectrum_profile_id, status, ln_atten_ds, ln_atten_us, act_atp_ds, act_atp_us, act_data_rate_ds, act_data_rate_us, attainable_rate_ds, attainable_rate_us, inp_ds, inp_us, es_ds, es_us, ses_ds, ses_us, init_times from vdsl_stats where time between '%s' and '%s' and init_times > 0 order by dslam_id, port_id, time """%(starttime,endtime)

        cursor.execute(query)
        rows = cursor.fetchall()

        i = 0
        stats = {}
        status = {}
        insert_data = []
        endtime = endtime.replace(minute=0)

        for row in rows:
            dslam_id = row[1]
            port_id = row[2]
            dslamportkey = (dslam_id<<14) + port_id
            try:
               stats[dslamportkey]
            except KeyError:
                # first entry for this port
                i = 0
                stats[dslamportkey] = {}
                stats[dslamportkey]['es_ds'] = []
                stats[dslamportkey]['ses_ds'] = []
                stats[dslamportkey]['es_us'] = []
                stats[dslamportkey]['ses_us'] = []
                stats[dslamportkey]['inits'] = []
                es_ds_ref = row[17]
                es_us_ref = row[18]
                ses_ds_ref = row[19]
                ses_us_ref = row[20]
                inits_ref = row[21]
                continue
            es_ds = row[17]
            es_us = row[18]
            ses_ds = row[19]
            ses_us = row[20]
            inits = row[21]
            stats[dslamportkey]['es_ds'].append(es_ds - es_ds_ref)
            es_ds_ref = es_ds
            stats[dslamportkey]['ses_ds'].append(ses_ds - ses_ds_ref)
            ses_ds_ref = ses_ds
            stats[dslamportkey]['es_us'].append(es_us - es_us_ref)
            es_us_ref = es_us
            stats[dslamportkey]['ses_us'].append(ses_us - ses_us_ref)
            ses_us_ref = ses_us
            stats[dslamportkey]['inits'].append(inits - inits_ref)
            inits_ref = inits

        for dslamportkey, errors in stats.iteritems():
            es_ds = errors['es_ds']
            ses_ds = errors['ses_ds']
            es_us = errors['es_us']
            ses_us = errors['ses_us']
            status[dslamportkey] = {'ds': {}, 'us': {}}
            dslam_id = dslamportkey >> 14
            port_id = dslamportkey % (dslam_id<<14)
            # 0 - white, 1 - orange, 2 - red, 3 - black
            status[dslamportkey]['ds'][0] = 0
            status[dslamportkey]['ds'][1] = 0
            status[dslamportkey]['ds'][2] = 0
            status[dslamportkey]['ds'][3] = 0
            status[dslamportkey]['ds'][4] = 0
            status[dslamportkey]['us'][0] = 0
            status[dslamportkey]['us'][1] = 0
            status[dslamportkey]['us'][2] = 0
            status[dslamportkey]['us'][3] = 0
            status[dslamportkey]['us'][4] = 0

            for i in range(len(es_ds)):
                if es_ds[i] < es_thresh_1 and ses_ds[i] < ses_thresh_1:
                    status[dslamportkey]['ds'][0] += 1
                elif es_ds[i] < es_thresh_2 and ses_ds[i] < ses_thresh_2:
                    status[dslamportkey]['ds'][1] += 1
                elif es_ds[i] < es_thresh_3 and ses_ds[i] < ses_thresh_3:
                    status[dslamportkey]['ds'][2] += 1
                elif es_ds[i] < es_thresh_4 and ses_ds[i] < ses_thresh_4:
                    status[dslamportkey]['ds'][3] += 1
                else:
                    status[dslamportkey]['ds'][4] += 1
            if len(es_ds) >= 8:
                es_ds_err_status = status[dslamportkey]['ds'][1] + status[dslamportkey]['ds'][2]*2 + status[dslamportkey]['ds'][3]*3 + status[dslamportkey]['ds'][4]*4
            else:
                es_ds_err_status = -1
            
            for i in range(len(es_us)):
                if es_us[i] < es_thresh_1 and ses_us[i] < ses_thresh_1:
                    status[dslamportkey]['us'][0] += 1
                elif es_us[i] < es_thresh_2 and es_us[i] < ses_thresh_2:
                    status[dslamportkey]['us'][1] += 1
                elif es_us[i] < es_thresh_3 and es_us[i] < ses_thresh_3:
                    status[dslamportkey]['us'][2] += 1
                elif es_us[i] < es_thresh_4 and es_us[i] < ses_thresh_4:
                    status[dslamportkey]['us'][3] += 1
                else:
                    status[dslamportkey]['us'][4] += 1
            if len(es_us) >= 8:
                es_us_err_status = status[dslamportkey]['us'][1] + status[dslamportkey]['us'][2]*2 + status[dslamportkey]['us'][3]*3 + status[dslamportkey]['us'][4]*4
            else:
                es_us_err_status = -1
            insert_data.append((endtime, dslam_id, port_id, es_ds_err_status, es_us_err_status))

        query = """insert into dslam_ports_err (time, dslam_id, port_id, err_ds_status, err_us_status)
                values (%s, %s, %s, %s, %s)"""
        cursor.executemany(query, insert_data)



class MA5600(object):

    def adsl_ports_err(self):
        endtime = datetime.datetime.now().replace(minute=29, second=0)
        query = """ select date_time_id,dslam_id,port_id,co_err_sec,cpe_err_sec,init_times from adsl_operation where date_time_id between (select min(date_time_id) from date_time where date_time > '%s') and (select max(date_time_id) from date_time where date_time < '%s') and init_times > 1 order by dslam_id, port_id, date_time_id """%(starttime, endtime)

        cursor.execute(query)
        rows = cursor.fetchall()

        i = 0
        stats = {}
        status = {}
        insert_data = []
        endtime = endtime.replace(minute=0)

        for row in rows:
            dslam_id = row[1]
            port_id = row[2]
            dslamportkey = (dslam_id<<14) + port_id
            try:
               stats[dslamportkey]
            except KeyError:
                # first entry for this port
                i = 0
                stats[dslamportkey] = {}
                stats[dslamportkey]['es_ds'] = []
                stats[dslamportkey]['es_us'] = []
                stats[dslamportkey]['inits'] = []
                es_ds_ref = row[4]
                es_us_ref = row[3]
                inits_ref = row[5]
                continue
            es_ds = row[4]
            es_us = row[3]
            inits = row[5]
            stats[dslamportkey]['es_ds'].append(es_ds - es_ds_ref)
            es_ds_ref = es_ds
            stats[dslamportkey]['es_us'].append(es_us - es_us_ref)
            es_us_ref = es_us
            stats[dslamportkey]['inits'].append(inits - inits_ref)
            inits_ref = inits

        for dslamportkey, errors in stats.iteritems():
            es_ds = errors['es_ds']
            es_us = errors['es_us']
            inits = errors['inits']
            status[dslamportkey] = {'ds': {}, 'us': {}}
            dslam_id = dslamportkey >> 14
            port_id = dslamportkey % (dslam_id<<14)

            # 0 - white, 1 - orange, 2 - red, 3 - black
            status[dslamportkey]['ds'][0] = 0
            status[dslamportkey]['ds'][1] = 0
            status[dslamportkey]['ds'][2] = 0
            status[dslamportkey]['ds'][3] = 0
            status[dslamportkey]['ds'][4] = 0
            status[dslamportkey]['us'][0] = 0
            status[dslamportkey]['us'][1] = 0
            status[dslamportkey]['us'][2] = 0
            status[dslamportkey]['us'][3] = 0
            status[dslamportkey]['us'][4] = 0


            for i in range(len(es_ds)):
                if es_ds[i] < es_thresh_1:
                    status[dslamportkey]['ds'][0] += 1
                elif es_ds[i] < es_thresh_2:
                    status[dslamportkey]['ds'][1] += 1
                elif es_ds[i] < es_thresh_3:
                    status[dslamportkey]['ds'][2] += 1
                elif es_ds[i] < es_thresh_4:
                    status[dslamportkey]['ds'][3] += 1
                else:
                    status[dslamportkey]['ds'][4] += 1
            if len(es_ds) >= 8:
                es_ds_err_status = status[dslamportkey]['ds'][1] + status[dslamportkey]['ds'][2]*2 + status[dslamportkey]['ds'][3]*3 + status[dslamportkey]['ds'][4]*4
            else:
                es_ds_err_status = -1


            for i in range(len(es_us)):
                if es_us[i] < es_thresh_1:
                    status[dslamportkey]['us'][0] += 1
                elif es_us[i] < es_thresh_2:
                    status[dslamportkey]['us'][1] += 1
                elif es_us[i] < es_thresh_3:
                    status[dslamportkey]['us'][2] += 1
                elif es_ds[i] < es_thresh_4:
                    status[dslamportkey]['us'][3] += 1
                else:
                    status[dslamportkey]['us'][4] += 1
            if len(es_us) >= 8:
                es_us_err_status = status[dslamportkey]['us'][1] + status[dslamportkey]['us'][2]*2 + status[dslamportkey]['us'][3]*3 + status[dslamportkey]['us'][4]*4
            else:
                es_us_err_status = -1
            insert_data.append((endtime,dslam_id, port_id, es_ds_err_status, es_us_err_status))

        query = """insert into dslam_ports_err (time, dslam_id, port_id, err_ds_status, err_us_status)
                values (%s, %s, %s, %s, %s)"""
        cursor.executemany(query, insert_data)




if __name__ == '__main__':
    getESThresholds6h()
    ma5600t = MA5600T()
    ma5600t.adsl_ports_err()
    ma5600t.vdsl_ports_err()
    ma5600 = MA5600()
    ma5600.adsl_ports_err()
#    adsl = threading.Thread(target=ma5600t.adsl_ports_status)
#    vdsl = threading.Thread(target=ma5600t.vdsl_ports_status)
#    adsl.start()
#    vdsl.start()
#    adsl.join()
#    vdsl.join()
