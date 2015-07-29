# Create your views here.

from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect
from dslam_stats.models import *
from datetime import date, timedelta
from django.db import connection
import re

def index(request):
    #neki kod
    dslams = Dslami.objects.exclude(ip='').order_by('name')
    slots = [0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15, 16] 
    ports = range(64)
    days = range(1,32)
    months = range(1,13)
    years = range((date.today() - timedelta(days=60)).year, date.today().year + 1)
    today = date.today()
    context = {'dslams': dslams, 'slots': slots, 'ports': ports,
        'days': days, 'months': months, 'years':years, 'today':today}
    return render(request, 'dslam_stats/index.html', context)

def detail(request, searchdate=date.today(), search="day"):
    #vidi u model kako dohvatiti statistiku porta
    dslams = Dslami.objects.exclude(ip='').order_by('name')
    slots = [0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15, 16] 
    ports = range(64)
    days = range(1,32)
    months = range(1,13)
    years = range((date.today() - timedelta(days=60)).year, date.today().year + 1)
    cursor = connection.cursor()
    dslam = request.GET['dslam']
    frame = int(request.GET['frame'])
    slot = int(request.GET['slot'])
    port = int(request.GET['port'])
    adsl = False
    vdsl = False
    try:
        day = int(request.GET['day'])
        month = int(request.GET['month'])
        year = int(request.GET['year'])
        searchdate = date(year, month, day)
    except KeyError: 
        pass
    try:
        search = request.GET['search']
    except KeyError:
        pass
    if search == "1 day":
        interval = 1
    elif search == "2 days":
        interval = 2
    elif search == "week":
        interval = 7
    elif re.search("month", search):
        interval = 30
    elif re.search("stb", search):
        return HttpResponseRedirect("http://stb.myorg.net/"+request.META['QUERY_STRING'])
    elif search == "lols":
        return HttpResponseRedirect("http://dslam_stats.myorg.net/dslam_stats/lols/detail?"+request.META['QUERY_STRING'])
    else:
        interval = 1
    end = searchdate + timedelta(days=1)
    start = searchdate - timedelta(days=interval)
    query = """select d.dslam_id, d.dslam_type_id, b.type from dslami d left join dslam_boards b on d.dslam_id=b.dslam_id where d.name='%s' and b.board='0/%s'"""%(dslam, slot)
    cursor.execute(query)
    row = cursor.fetchone()
    if row is None:
        context = {'dslams': dslams, 'slots': slots, 'ports': ports,
          'days': days, 'months': months, 'years':years, 'searchdate':searchdate,
          'stats':None, 'dslam':None, 'frame':None, 'slot':None, 'port':None,
          'vdsl': vdsl, 'adsl':adsl}
        return render(request, 'dslam_stats/detail.html', context)
    dslam_id = row[0]
    dslam_type = row[1]
    board_type = row[2]
    if dslam_type in (2,3,4,5) and board_type in ('H80BVDPM', 'HS3BVCMM'):
    # vdsl_stats
        portname = '%s/%s/%s'%(frame, slot, port)
        port_id = Port.objects.filter(name=portname).filter(type=2)[0].id
        query = """select time, vs.name, vrd.name, vru.name, vi.name, vsnr.name, v.status, v.modulation, v.inp_ds, v.inp_us, rtxds.name, rtxus.name, v.ln_atten_ds, v.ln_atten_us, v.kl0_co, v.kl0_cpe, v.snr_margin_ds, v.snr_margin_us, v.act_atp_ds, v.act_atp_us, v.attainable_rate_ds, v.attainable_rate_us, v.act_data_rate_ds, v.act_data_rate_us, v.act_delay_ds, v.act_delay_us, v.es_ds, v.es_us, v.ses_ds, v.ses_us, v.init_times  from vdsl_stats v left join vop_spectrum vs on vs.id=v.line_spectrum_profile_id left join vop_rate vrd on vrd.id=v.data_rate_profile_ds_id left join vop_rate vru on vru.id=v.data_rate_profile_us_id left join vop_inp vi on vi.id=v.inp_profile_id left join vop_snr vsnr on vsnr.id=v.noise_margin_profile_id left join xdsl_rtx rtxds on rtxds.id=v.rtx_used_ds left join xdsl_rtx rtxus on rtxus.id=rtx_used_us where time between '%s' and '%s' and dslam_id=%s and port_id=%s order by time DESC"""%(start, end, dslam_id, port_id)
        cursor.execute(query)
        rows = cursor.fetchall()
        vdsl = True
        stats = []
        modulation = {
          "-1": "-",
          "0":  "-",
          "1":  "ADSL",
          "2":  "ADSL2",
          "3":  "ADSL2+",
          "4":  "VDSL",
          "5":  "VDSL2"
        } 
        for i in range(0,len(rows)-1):
            stat = []
            status = rows[i][6]
            stat.append(str(rows[i][0]))
            stat.append(str(rows[i][1]))
            stat.append(str(rows[i][2]))
            stat.append(str(rows[i][3]))
            stat.append(str(rows[i][4]))
            stat.append(str(rows[i][5]))
            status = rows[i][6]
            if status == 0:
                stat.append(modulation[str(rows[i][7])])
                stat.append(str(rows[i][8]))
                stat.append(str(rows[i][9]))
                stat.append(str(rows[i][10]))
                stat.append(str(rows[i][11]))
                stat.append(str(rows[i][12]))
                stat.append(str(rows[i][13]))
                stat.append(str(rows[i][14]))
                stat.append(str(rows[i][15]))
                stat.append(str(rows[i][16]))
                stat.append(str(rows[i][17]))
                stat.append(str(rows[i][18]))
                stat.append(str(rows[i][19]))
                stat.append(str(rows[i][20]))
                stat.append(str(rows[i][21]))
                stat.append(str(rows[i][22]))
                stat.append(str(rows[i][23]))
                stat.append(str(rows[i][24]))
                stat.append(str(rows[i][25]))
            else:
                stat.append(modulation["-1"])
                for j in range(8,26):
                    stat.append(0)
            stat.append(str(rows[i][26] - rows[i+1][26]))
            stat.append(str(rows[i][27] - rows[i+1][27]))
            stat.append(str(rows[i][28] - rows[i+1][28]))
            stat.append(str(rows[i][29] - rows[i+1][29]))
            stat.append(str(rows[i][30] - rows[i+1][30]))
            stats.append(tuple(stat))
    elif dslam_type in (2,3,4) and board_type in ('H808ADPM', 'H802ADQD'):
        portname = '%s/%s/%s'%(frame, slot, port)
        port_id = Port.objects.filter(name=portname).filter(type=1)[0].id
        query = """select a.time, vs.name, vrd.name, vru.name, vi.name, vsnr.name, a.status, a.inp_ds, a.inp_us, rtx.name, a.ln_atten_ds, a.ln_atten_us, a.snr_margin_ds, a.snr_margin_us, a.act_atp_ds, a.act_atp_us, a.attainable_rate_ds, a.attainable_rate_us, a.act_data_rate_ds, a.act_data_rate_us, a.act_delay_ds, a.act_delay_us, a.es_ds, a.es_us, a.ses_ds, a.ses_us, a.init_times  from adsl_stats a left join vop_spectrum vs on vs.id=a.line_spectrum_profile_id left join vop_rate vrd on vrd.id=a.data_rate_profile_ds_id left join vop_rate vru on vru.id=a.data_rate_profile_us_id left join vop_inp vi on vi.id=a.inp_profile_id left join vop_snr vsnr on vsnr.id=a.line_spectrum_profile_id left join xdsl_rtx rtx on rtx.id=a.rtx_used_ds where time between '%s' and '%s' and dslam_id=%s and port_id=%s order by time DESC"""%(start, end, dslam_id, port_id)
        cursor.execute(query)
        rows = cursor.fetchall()
        adsl = True
        stats = []
        for i in range(0,len(rows)-1):
            stat = []
            status = rows[i][6]
            stat.append(str(rows[i][0]))
            stat.append(str(rows[i][1]))
            stat.append(str(rows[i][2]))
            stat.append(str(rows[i][3]))
            stat.append(str(rows[i][4]))
            stat.append(str(rows[i][5]))
            status = rows[i][6]
            if status == 0:
                stat.append(str(rows[i][7])) #inp
                stat.append(str(rows[i][8]))
                stat.append(str(rows[i][9])) #rtx_used_ds
                stat.append(str(rows[i][10])) #ln_atten
                stat.append(str(rows[i][11]))
                stat.append(str(rows[i][12])) #snr_margin
                stat.append(str(rows[i][13])) 
                stat.append(str(rows[i][14])) #act_atp
                stat.append(str(rows[i][15]))
                stat.append(str(rows[i][16])) # attainable_rate
                stat.append(str(rows[i][17]))
                stat.append(str(rows[i][18])) #act_data_rate
                stat.append(str(rows[i][19]))
                stat.append(str(rows[i][20])) #act_delay
                stat.append(str(rows[i][21]))
            else:
                for j in range(7,22):
                    stat.append(0)
            stat.append(str(rows[i][22] - rows[i+1][22]))
            stat.append(str(rows[i][23] - rows[i+1][23]))
            stat.append(str(rows[i][24] - rows[i+1][24]))
            stat.append(str(rows[i][25] - rows[i+1][25]))
            stat.append(str(rows[i][26] - rows[i+1][26]))
            stats.append(tuple(stat))
    context = {'dslams': dslams, 'slots': slots, 'ports': ports,
        'days': days, 'months': months, 'years':years, 'searchdate':searchdate,
        'stats':stats, 'dslam':dslam, 'frame':frame, 'slot':slot, 'port':port,
        'vdsl': vdsl, 'adsl':adsl}
    return render(request, 'dslam_stats/detail.html', context)


