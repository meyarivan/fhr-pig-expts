

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

from com.google.common.hash import Hashing
import hashlib
import random
import traceback
import zlib


HASHFUNC = Hashing.murmur3_128()
#HASHFUNC = Hashing.sipHash24()
#HASHFUNC = Hashing.sha1()

def loads(strx):
    return JSON.parseObject(strx, Map)

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
    checksums = []
    for d in dictx:
        sinfo = dictx.get(d).get('org.mozilla.appSessions.previous')

        if not sinfo:
            continue

        ck = HASHFUNC.newHasher().putString(d + sinfo.toString()).hash().toString()
        checksums.append(ck)

    return checksums

def get_session_data(dictx):
    if not dictx:
        return -1, -1, -1, -1, -1, -1

    dg = dictx.getString
    default = "-1"

    return (dg('startDay') or default,
            dg('activeTicks') or default,
            dg('totalTime') or default,
            dg('main') or default,
            dg('firstPaint') or default,
            dg('sessionRestored') or default)


invalid_record = (False, 0, '', '', '0', '0', '0', '0', '0','0','', 0, 0, 'U/A', -1, -1, -1, -1, -1, -1)

reporter = PigCounterHelper()
udf_context = UDFContext.getUDFContext()


creation_date_cache = {}
    
@outputSchema('meta:bag{t:tuple(valid:boolean, version:int, lastping:chararray, thisping:chararray, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, platform_build_id:chararray, app_build_id:chararray, checksum:chararray, ndays:int, ndays_with_session_data:int, country:chararray, start_day:int, active_ticks:int, total_time:int, main:int, first_paint:int, session_restored:int)}')
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

        ndays = len(x['data']['days'])
        checksums = get_days_data(days_data)
        ndays_with_session_data = len(checksums)

        retval = []
        for i in checksums:
            s = (True, fhr_version, x['lastPingDate'], x['thisPingDate']) + get_gai_fields(gai) + \
                (i, ndays, ndays_with_session_data)+ (x['geoCountry'],) + tuple(map(int, get_session_data(x['data']['last']['org.mozilla.appSessions.current'])))

            retval.append(s)

        return retval

    except:
        print >> sys.stderr, 'Exception (ignored)',  sys.exc_info()[0], sys.exc_info()[1]
        traceback.print_exc(file = sys.stderr)
        reporter.incrCounter('stats', 'errors', 1)
        return invalid_record

@outputSchema('t:tuple(id:chararray, valid:boolean, version:int, lastping:chararray, thisping:chararray, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, platform_build_id:chararray, app_build_id:chararray, checksum:chararray, ndays:int, ndays_with_session_data:int, country:chararray, start_day:int, active_ticks:int, total_time:int, main:int, first_paint:int, session_restored:int)')
def get_hr(bagx):
    retval = None

    for r in bagx:
        if retval == None:
            retval = r
            continue

        if r[4] > retval[4]:
            retval = r
        elif r[4] == retval[4]:
            if r[13] > retval[13]:
                retval = r
            elif r[13] == retval[13]:
                if r[17] > retval[17]:
                    retval = r

    return retval



