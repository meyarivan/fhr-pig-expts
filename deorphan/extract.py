

import os, sys

import com.alibaba.fastjson.JSON as JSON
import java.util.Map as Map
from datetime import datetime, timedelta
import org.python.core.PyDictionary as PyDictionary
import java.util.HashMap
import time

import org.apache.pig.tools.pigstats.PigStatusReporter as PigStatusReporter
import org.apache.pig.tools.counters.PigCounterHelper as PigCounterHelper
import org.apache.pig.impl.util.UDFContext as UDFContext

import hashlib
import random
import traceback
import zlib

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

    moz_version = dictx.getString('version') or 'UA'
    platform_version = dictx.getString('updateChannel') or 'UA'
    os = dictx.getString('os') or 'UA'
    xpcomabi = dictx.getString('xpcomabi') or 'UA'
    pbid = dictx.getString('platformBuildID') or 'UA'
    abid = dictx.getString('appBuildID') or 'UA'
    s = (moz_version, platform_version, os, xpcomabi, pbid, abid)
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
    days_with_session_data = []

    for d in dictx:
        ndays += 1
        di = date_to_int(d)
        if ( di == -1):
            continue
     
        if not dictx.get(d).get('org.mozilla.appSessions.previous'):
            continue
        
        if (di >= last_valid_date):
            days_with_session_data.append((di, d))

            if  (di < chosen_date):
                chosen_date = di
                chosen_date_str = d

    if chosen_date_str is not None:
        days_with_session_data.sort()
        checksum = hashlib.sha1((chosen_date_str + dictx[chosen_date_str].toJSONString()).encode('utf-8')).hexdigest()

        checksum_all = '%s%s' % (checksum, ''.join(map(hex, [zlib.crc32('%s%s' %(i[1], dictx[i[1]].toJSONString())) & 0xffffffff for i in days_with_session_data[1:]])))

        return checksum, checksum_all, chosen_date_str, ndays, len(days_with_session_data)
    else:
        return '', '', '', ndays, len(days_with_session_data)


initialize_date_lookup()
invalid_record = (False, 0, 0, 0, '0', '0', '0', '0', '0','0','', '', '', 0, 0, 'U/A')

reporter = PigCounterHelper()
udf_context = UDFContext.getUDFContext()


creation_date_cache = {}
    
@outputSchema('t:tuple(valid:boolean, version:int, last:int, this:int, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, platform_build_id:chararray, app_build_id:chararray, checksum:chararray, checksum_all:chararray, chosen_date:chararray, ndays:int, ndays_with_session_data:int, country:chararray)')
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
            reporter.incrCounter('stats', 'fhrv%s' % fhr_version, 1)
            return invalid_record

        s = (True, fhr_version, date_to_int(x['lastPingDate']), date_to_int(x['thisPingDate'])) + get_gai_fields(gai) + get_days_data(days_data) + (x['geoCountry'],)
        return s

    except:
        print >> sys.stderr, 'Exception (ignored)',  sys.exc_info()[0], sys.exc_info()[1]
        traceback.print_exc(file = sys.stderr)
        reporter.incrCounter('stats', 'errors', 1)
        return invalid_record

@outputSchema("headrecords:bag{t:tuple(id:chararray, valid:boolean, version:int, last:int, this:int, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, platform_build_id:chararray, app_build_id:chararray, checksum:chararray, checksum_all:chararray, chosen_date:chararray, ndays:int, ndays_with_session_data:int, country:chararray)}")
def get_head_records(reports, cnt, nrec):
    # reports is a bag => iterator which returns tuples
    # cnt is the number of reports
    # nrec is max number of reports to be chosen from 'reports'

    cnt = int(cnt)
    nrec = int(nrec)

    if cnt == 1:
        return reports[:]

    headset = []
    nc = 0
    frag_cache = {}
    frag_lens = {}

    for r in reports:
        if len(headset) >= nrec:
            break
            
        orphaned = False
        rc = r[12]
        rcl = len(rc)

        if rcl not in frag_lens: # update frag_cache with fragments of new len/size rcl
            frag_lens[rcl] = None
            for f in headset:
                frag_cache[f[12][:rcl]] = None

        if rc not in frag_cache:
            headset.append(r)

            for i in frag_lens:
                frag_cache[rc[:i]] = None


    print >> sys.stderr, "HEADSET size", len(headset), "cnt", cnt
    return headset


    
    
