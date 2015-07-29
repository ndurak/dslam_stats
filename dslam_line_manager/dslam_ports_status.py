#!/usr/bin/python

import datetime
import MySQLdb
from decimal import *
from ConfigParser import ConfigParser

getcontext().prec=1
getcontext().rounding=ROUND_FLOOR

conn = MySQLdb.connect (host = "rpmiptvdb.zg.iskon.hr",
                        user = "dslam",
                        passwd = "stats",
                        db = "dslam")
conn.autocommit(True)
cursor = conn.cursor()

yesterday = (datetime.datetime.now() - datetime.timedelta(hours=24)).replace(hour=0, minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
last_week = (datetime.datetime.now() - datetime.timedelta(days=7)).replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")

config = ConfigParser()
config.read('dsl_line_manager.conf')




def getLolsThresh(*args,**kwargs):
    global lthresh_y
    global lthresh_o
    global lthresh_r
    global lthresh_b
    if len(args) == 4:
        if args[0] < args[1] < args[2] < args[3]:
            lthresh_y = args[0]
            lthresh_o = args[1]
            lthresh_r = args[2]
            lthresh_b = args[3]
        else:
            lthresh_y = int(config.get("Lols thresholds", "lthresh_y"))
            lthresh_o = int(config.get("Lols thresholds", "lthresh_o"))
            lthresh_r = int(config.get("Lols thresholds", "lthresh_r"))
            lthresh_b = int(config.get("Lols thresholds", "lthresh_b"))
    else:
        lthresh_y = int(config.get("Lols thresholds", "lthresh_y"))
        lthresh_o = int(config.get("Lols thresholds", "lthresh_o"))
        lthresh_r = int(config.get("Lols thresholds", "lthresh_r"))
        lthresh_b = int(config.get("Lols thresholds", "lthresh_b"))



class DslamPortStatus(object):

    def __init__(self):
        self.stats = {}
        self.errors = {}
        self.lols = {}
        self.status = {}
        self._getAdslStatsMa5600()
        self._getAdslStats()
        self._getVdslStats()
        self._getLols()
        self._getErrorStatus()


    def _getErrorStatus(self):
        query = """select time, dslam_id, port_id, err_ds_status, err_us_status from dslam_ports_err where time > '%s' order by dslam_id, port_id, time"""%last_week
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            dslam_id = row[1]
            port_id = row[2]
            dslamportkey = (dslam_id<<14) + port_id
            try:
                self.errors[dslamportkey]
            except KeyError:
                self.errors[dslamportkey] = {}
                self.errors[dslamportkey]['ds'] = []
                self.errors[dslamportkey]['us'] = []
            self.errors[dslamportkey]['ds'].append(row[3])
            self.errors[dslamportkey]['us'].append(row[4])

        for dslamportkey, es in self.errors.iteritems():
            es_ds_list = es['ds']
            es_us_list = es['us']
            black = 0
            red = 0
            orange = 0
            yellow = 0
            green = 0
            if len(es_ds_list) >= 8:
                for es_interval in es_ds_list:
                    if es_interval > 36:
                        black += 1
                    elif es_interval > 24:
                        red += 1
                    elif es_interval > 12:
                        orange += 1
                    elif es_interval > 6:
                        yellow += 1
                    else:
                        green += 1
                es_ds = black*4 + red*3 + orange*2 + yellow*1
                if es_ds > 84:
                    es_ds_status = 4
                elif es_ds > 56:
                    es_ds_status = 3
                elif es_ds > 28:
                    es_ds_status = 2
                elif es_ds > 14:
                    es_ds_status = 1
                else:
                    es_ds_status = 0
            else:
#                print len(es_ds_list)
                es_ds_status = -1
            black = 0
            red = 0
            orange = 0
            yellow = 0
            green = 0
            if len(es_us_list) >= 8:
                for es_interval in es_us_list:
                    if es_interval > 36:
                        black += 1
                    elif es_interval > 24:
                        red += 1
                    elif es_interval > 12:
                        orange += 1
                    elif es_interval > 6:
                        yellow += 1
                    else:
                        green += 1
                es_us = black*4 + red*3 + orange*2 + yellow*1
                if es_us > 84:
                    es_us_status = 4
                elif es_us > 56:
                    es_us_status = 3
                elif es_us > 28:
                    es_us_status = 2
                elif es_us > 14:
                    es_us_status = 1
                else:
                    es_us_status = 0
            else:
                es_us_status = -1
            self.errors[dslamportkey]['es_ds'] = es_ds_status
            self.errors[dslamportkey]['es_us'] = es_us_status


    def _getLols(self):
        query = """select l.time, l.dslam_id, port_id, lols from dslam_ports_lolscount l 
                join dslami d on d.dslam_id=l.dslam_id where l.time > '%s'"""%last_week
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            dslam_id = row[1]
            port_id = row[2]
            dslamportkey = (dslam_id<<14) + port_id
            try:
                self.lols[dslamportkey]
            except KeyError:
                self.lols[dslamportkey] = {}
                self.lols[dslamportkey]['lols'] = []
            self.lols[dslamportkey]['lols'].append(row[3])

        for dslamportkey,event in self.lols.iteritems():
            black = 0
            red = 0
            orange = 0
            yellow = 0
            for lolcount in event['lols']:
                if lolcount > lthresh_b:
                    black += 1
                elif lolcount >lthresh_r:
                    red += 1
                elif lolcount > lthresh_o:
                    orange += 1
                elif lolcount > lthresh_y:
                    yellow += 1
            points = black*4 + red*3 + orange*2 + yellow*1
            if points >= 84:  #56
                self.lols[dslamportkey]['status'] = 4
            elif points >= 56:    #33
                self.lols[dslamportkey]['status'] = 3
            elif points >= 28:    #16
                self.lols[dslamportkey]['status'] = 2
            elif points >= 14:     # 8
                self.lols[dslamportkey]['status'] = 1
            else:
                self.lols[dslamportkey]['status'] = 0 



    def _getAdslStatsMa5600(self):
        query = """select dslam_id,port_id,min(max_d_rate),min(max_u_rate),avg(d_stream_atten),avg(u_stream_atten),avg(d_noise_marg),avg(u_noise_marg), avg(max_d_rate), avg(max_u_rate) from adsl_operation where max_d_rate > 0 and date_time_id > (select max(date_time_id) from date_time) - %s group by dslam_id, port_id"""%'48'
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            dslam_id = row[0]
            port_id = row[1]
            dslamportkey = (dslam_id<<14) + port_id
            self.stats[dslamportkey] = {}
            self.stats[dslamportkey]['max_d_rate'] = row[2]
            self.stats[dslamportkey]['max_u_rate'] = row[3]
            self.stats[dslamportkey]['att_ds'] = row[4]
            self.stats[dslamportkey]['att_us'] = row[5]
            self.stats[dslamportkey]['snr_ds'] = row[6]
            self.stats[dslamportkey]['snr_us'] = row[7]
            self.stats[dslamportkey]['max_d_rate_avg'] = row[8]
            self.stats[dslamportkey]['max_u_rate_avg'] = row[9]



    def _getAdslStats(self):
        query = """select dslam_id, port_id, min(attainable_rate_ds), min(attainable_rate_us), avg(ln_atten_ds), avg(ln_atten_us),avg(snr_margin_ds), avg(snr_margin_us), avg(attainable_rate_ds), avg(attainable_rate_us) from adsl_stats where status = 0 and attainable_rate_ds > 0 and time > '%s' group by dslam_id, port_id"""%yesterday
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            dslam_id = row[0]
            port_id = row[1]
            dslamportkey = (dslam_id<<14) + port_id
            self.stats[dslamportkey] = {}
            self.stats[dslamportkey]['max_d_rate'] = row[2]
            self.stats[dslamportkey]['max_u_rate'] = row[3]
            self.stats[dslamportkey]['att_ds'] = row[4]
            self.stats[dslamportkey]['att_us'] = row[5]
            self.stats[dslamportkey]['snr_ds'] = row[6]
            self.stats[dslamportkey]['snr_us'] = row[7]
            self.stats[dslamportkey]['max_d_rate_avg'] = row[8]
            self.stats[dslamportkey]['max_u_rate_avg'] = row[9]



    def _getVdslStats(self):
        query = """select dslam_id, port_id, avg(attainable_rate_ds), avg(attainable_rate_us), avg(ln_atten_ds), avg(ln_atten_us),avg(snr_margin_ds), avg(snr_margin_us), avg(attainable_rate_ds), avg(attainable_rate_us), max(modulation) from vdsl_stats where status = 0 and attainable_rate_ds > 0 and time > '%s' group by dslam_id, port_id"""%yesterday
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            dslam_id = row[0]
            port_id = row[1]
            dslamportkey = (dslam_id<<14) + port_id
            self.stats[dslamportkey] = {}
            self.stats[dslamportkey]['max_d_rate'] = row[2]
            self.stats[dslamportkey]['max_u_rate'] = row[3]
            self.stats[dslamportkey]['att_ds'] = row[4]
            self.stats[dslamportkey]['att_us'] = row[5]
            self.stats[dslamportkey]['snr_ds'] = row[6]
            self.stats[dslamportkey]['snr_us'] = row[7]
            self.stats[dslamportkey]['max_d_rate_avg'] = row[8]
            self.stats[dslamportkey]['max_u_rate_avg'] = row[9]



    def save_to_db(self):
        insert_data = []
        for dslamportkey, data in self.stats.iteritems():
            dslam_id = dslamportkey >> 14
            port_id = dslamportkey % (dslam_id<<14)
            try:
                self.status[dslamportkey]
            except KeyError:
                self.status[dslamportkey] = {}
                self.status[dslamportkey]['es_ds'] = 0 
                self.status[dslamportkey]['es_us'] = 0
                self.status[dslamportkey]['lols'] = 0
            self.status[dslamportkey]['max_d_rate'] = int(round(data['max_d_rate'],0))
            self.status[dslamportkey]['max_u_rate'] = int(round(data['max_u_rate'],0))
            self.status[dslamportkey]['max_d_rate_avg'] = int(round(data['max_d_rate_avg'],0))
            self.status[dslamportkey]['max_u_rate_avg'] = int(round(data['max_u_rate_avg'],0))
            self.status[dslamportkey]['att_ds'] = round(data['att_ds'],1)
            self.status[dslamportkey]['att_us'] = round(data['att_us'],1)

        for dslamportkey, data in self.errors.iteritems():
            try:
                self.status[dslamportkey]
            except KeyError:
                self.status[dslamportkey] = {}
                self.status[dslamportkey]['max_d_rate'] = 0
                self.status[dslamportkey]['max_u_rate'] = 0
                self.status[dslamportkey]['max_d_rate_avg'] = 0
                self.status[dslamportkey]['max_u_rate_avg'] = 0
                self.status[dslamportkey]['att_ds'] = 0
                self.status[dslamportkey]['att_us'] = 0
                self.status[dslamportkey]['lols'] = 0
            self.status[dslamportkey]['es_ds'] = data['es_ds']
            self.status[dslamportkey]['es_us'] = data['es_us']

        for dslamportkey, data in self.lols.iteritems():
            try:
                self.status[dslamportkey]
            except KeyError:
                self.status[dslamportkey] = {}
                self.status[dslamportkey]['max_d_rate'] = 0
                self.status[dslamportkey]['max_u_rate'] = 0
                self.status[dslamportkey]['max_d_rate_avg'] = 0
                self.status[dslamportkey]['max_u_rate_avg'] = 0
                self.status[dslamportkey]['att_ds'] = 0
                self.status[dslamportkey]['att_us'] = 0
                self.status[dslamportkey]['es_ds'] = 0
                self.status[dslamportkey]['es_us'] = 0
            self.status[dslamportkey]['lols'] = data['status'] 

        for dslamportkey, data in self.status.iteritems():
            dslam_id = dslamportkey >> 14
            port_id = dslamportkey % (dslam_id<<14)
            insert_data.append((dslam_id,port_id,data['max_d_rate'],data['max_u_rate'],data['max_d_rate_avg'],data['max_u_rate_avg'],data['att_ds'],data['att_us'],data['es_ds'],data['es_us'],data['lols']))
#        for dslamportkey, data in self.stats.iteritems():
#            dslam_id = dslamportkey >> 14
#            port_id = dslamportkey % (dslam_id<<14)
#            max_d_rate = str(data['max_d_rate'])
#            max_u_rate = str(data['max_u_rate'])
#            att_ds = str(round(data['att_ds'],1))
#            att_us = str(round(data['att_us'],1))
#            try:
                # es means error statuses, not error seconds
#                es_ds_list = self.errors[dslamportkey]['ds']
#                es_us_list = self.errors[dslamportkey]['us']
#            except KeyError:
#                insert_data.append((dslam_id, port_id, str(max_d_rate), str(max_u_rate), str(att_ds), str(att_us), -1, -1))
#                continue
            # there must be data for minimum 2 days in a week
#            black = 0
#            red = 0
#            orange = 0
#            yellow = 0
#            green = 0
#            if len(es_ds_list) >= 8:
#                for es_interval in es_ds_list:
#                    if es_interval > 20:
#                        black += 1
#                    elif es_interval > 15:
#                        red += 1
#                    elif es_interval > 10:
#                        orange += 1
#                    elif es_interval > 5:
#                        yellow += 1
#                    else:
#                        green += 1
#                es_ds = black*4 + red*3 + orange*2 + yellow*1
#                if es_ds > 60:
#                    es_ds_status = 4
#                elif es_ds > 36:
#                    es_ds_status = 3
#                elif es_ds > 20:
#                    es_ds_status = 2
#                elif es_ds > 12:
#                    es_ds_status = 1
#                else:
#                    es_ds_status = 0
#            else:
#                print len(es_ds_list)
#                es_ds_status = -1
#            black = 0
#            red = 0
#            orange = 0
#            yellow = 0
#            green = 0
#            if len(es_us_list) >= 8:
#                for es_interval in es_us_list:
#                    if es_interval > 20:
#                        black += 1
#                   elif es_interval > 15:
#                        red += 1
#                    elif es_interval > 10:
#                        orange += 1
#                    elif es_interval > 5:
#                        yellow += 1
#                    else:
#                        green += 1
#                es_us = black*4 + red*3 + orange*2 + yellow*1
#                if es_us > 60:
#                    es_us_status = 4
#                elif es_us > 36:
#                    es_us_status = 3
#                elif es_us > 20:
#                    es_us_status = 2
#                elif es_us > 12:
#                    es_us_status = 1
#                else:
#                    es_us_status = 0
#            else:
#                es_us_status = -1
#            insert_data.append((dslam_id, port_id, str(max_d_rate), str(max_u_rate), str(att_ds), str(att_us), es_ds_status, es_us_status))

        query = """insert into dslam_ports_status (dslam_id,port_id,max_d_rate,max_u_rate,max_d_rate_avg,max_u_rate_avg,att_ds,att_us,es_ds,es_us,lols) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#        print insert_data
#        print len(insert_data)
        cursor.executemany(query, insert_data)




if __name__ == '__main__':
    getLolsThresh()
    dps = DslamPortStatus()
    dps.save_to_db()

