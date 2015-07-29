from django.shortcuts import render
from django.http import HttpResponse
import logging
import json
import inspect
import meltapi

rpc_errors = {
    -32700: 'Parse error',
    -32600: 'Invalid Request',
    -32601: 'Method not found',
    -32602: 'Invalid params',
    -32603: 'Internal error',
    -32000: 'Application error',
    -32001: 'No SNMP response',
    -1: 'Integrity error',
    -2: 'Auth error',
    -3: 'Not allowed',
    -4: 'Second login',
    -5: 'Second login failed',
    -6: 'Forced logout'
}

meltRpcCall = {}
meltmethods = inspect.getmembers(meltapi, inspect.isfunction)
for f in meltmethods:
    meltRpcCall[f[0]] = f[1]
# Create your views here.

logger = logging.getLogger('dostupnost')

def index(request):
    if request.META['CONTENT_TYPE'] != "application/json":
        return HttpResponse(status=415)
    rpcresp = {}
    try:
        rpc = json.loads(request.body)
        reqid = rpc['id']
        rpcresp['id'] = reqid
        method = rpc['method']
        rpcresp['method'] = rpc['method']
        params = rpc['params']
    except Exception, e:
        logger.error("%s: %s"%(type(e).__name__, e))
        logger.error(request.body)
        rpcresp['error'] = {'code': -32600, 'message': 'invalid request'}
        return HttpResponse(json.dumps(rpcresp), content_type = 'application/json')
    json.dumps(rpcresp)
    result = meltRpcCall[method](params)
    json.dumps(rpcresp)
    json.dumps(result)
    if result < 0:
        rpcresp['error'] = result
    else:
        rpcresp['result'] = result
    return HttpResponse(json.dumps(rpcresp), content_type = "application/json")


