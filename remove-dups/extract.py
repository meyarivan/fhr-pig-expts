

import os, sys
from datetime import datetime, timedelta
import hashlib
import random

import com.alibaba.fastjson.JSON as JSON
import java.util.Map as Map
import org.python.core.PyDictionary as PyDictionary
import java.util.HashMap

import org.apache.pig.tools.pigstats.PigStatusReporter as PigStatusReporter
import org.apache.pig.tools.counters.PigCounterHelper as PigCounterHelper
import org.apache.pig.impl.util.UDFContext as UDFContext


VALID_DATES = None
MAX_VALID_DATE = -1
MIN_VALID_DATE = -1

START_DATE = UDFContext.getUDFContext().getClientSystemProps().get("start_date")


def initialize_date_lookup():
    global VALID_DATES, MAX_VALID_DATE, MIN_VALID_DATE

    valid_date = datetime(2013,5,23)
    delta = timedelta(days = 1)
    all = {}
    max_valid_date = datetime.today() + timedelta(days = 2)

    while valid_date <= max_valid_date:
        all[int(valid_date.strftime('%Y%m%d'))] = None
        valid_date += delta
        
    VALID_DATES = all

    MAX_VALID_DATE = int(max_valid_date.strftime('%Y%m%d'))
    MIN_VALID_DATE = int(datetime(2013,5,23).strftime('%Y%m%d'))

    
    
def loads(strx):
    return JSON.parseObject(strx, Map)


def date_to_int(datex):

    if (datex is None ) or (len(datex) != 10) or ((datex[4] != '-') or (datex[7] != '-')):
        return -1

    p = datex.split('-')
    i = int('%s%s%s' % (p[0], p[1], p[2]))

    if i in VALID_DATES:
        return i
    elif i < MIN_VALID_DATE:
        return 0
    elif i > MAX_VALID_DATE:
        return 99999


def get_gai_fields(dictx):
    if dictx is None:
        dictx = {}

    moz_version = dictx.get('version') or 'UA'
    platform_version = dictx.get('updateChannel') or 'UA'
    os = dictx.get('os') or 'UA'
    xpcomabi = dictx.get('xpcomabi') or 'UA'

    s = (moz_version, platform_version, os, xpcomabi)
    
    return s
    

def parse_addon_data(ads):
    retval = []

    for k in ads:
        if (k == '_v'):
            continue

        v = ads.get(k)

        dv = {}
        for l in v:
            dv[l] = str(v[l])
        dv['name'] = k

        retval.append((dv,))

    return retval

def parse_sysinfo(dictx):
    if not dictx:
        dictx = {}

    cpus = dictx.get('cpuCount') or 0
    memory_in_mb = dictx.get('memoryMB') or 0
    arch = dictx.get('architecture') or 'UA'

    return int(cpus), int(memory_in_mb), str(arch)

def get_days_data(dictx):
    today = apply(datetime, map(int, START_DATE.split('-')))
    last_valid_date = date_to_int(((today - timedelta(days = 180)).strftime("%Y-%m-%d"))) # TODO: check date math.
    
    chosen_date = 30000000 #silly max no
    chosen_date_str = None
    
    ndays = 0

    for d in dictx:
        ndays += 1
        di = date_to_int(d)
        if ( di == -1):
            continue
     
        if not dictx.get(d).get('org.mozilla.appSessions.previous'):
            continue

        if (di >= last_valid_date) and (di < chosen_date):
            chosen_date = di
            chosen_date_str = d

    if chosen_date_str is not None:
        return hashlib.sha1(chosen_date_str + dictx.get(chosen_date_str).toJSONString()).hexdigest(), chosen_date_str, ndays
    else:
        return '', '', ndays


initialize_date_lookup()
invalid_record = (False, 0, 0, 0, '0', '0', '0', '0', '', '', 0)

reporter = PigCounterHelper()
udf_context = UDFContext.getUDFContext()


@outputSchema('t:tuple(valid:boolean, version:int, last:int, this:int, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, checksum:chararray, chosen_date:chararray, ndays:int)')
def process(line):
    try:
        x = loads(line)
        fhr_version = int(x['version'])
        gai = days_data = sysinfo = None

        if fhr_version == 2:
            reporter.incrCounter('stats', 'fhrv2', 1)
            gai = x['geckoAppInfo']
            addons = x['data']['last']['org.mozilla.addons.active']
            sysinfo = x['data']['last']['org.mozilla.sysinfo.sysinfo']

            days_data = x['data']['days']
        else:
            reporter.incrCounter('stats', 'v3records', 1)
            return invalid_record

        # find first date >= today - 180 days

        s = (True, fhr_version, date_to_int(x['lastPingDate']), date_to_int(x['thisPingDate'])) + get_gai_fields(gai) + get_days_data(days_data)
        return s

    except Exception, e:
        print >> sys.stderr, 'INVALID', e
        reporter.incrCounter('stats', 'errors', 1)
        return invalid_record

@outputSchema('entries:bag{t:tuple(id:bytearray, valid:boolean, version:int, last:int, this:int, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, checksum:chararray, chosen_date:chararray, ndays:int, val:bytearray)}')
def choose_valid_reports(entries):
    return([random.choice(entries)])

