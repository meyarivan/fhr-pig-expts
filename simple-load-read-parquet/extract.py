

import os, sys

import com.alibaba.fastjson.JSON as JSON
import java.util.Map as Map
from datetime import datetime, timedelta
import org.python.core.PyDictionary as PyDictionary
import java.util.HashMap

import org.apache.pig.tools.pigstats.PigStatusReporter as PigStatusReporter
import org.apache.pig.tools.counters.PigCounterHelper as PigCounterHelper


VALID_DATES = None
MAX_VALID_DATE = -1
MIN_VALID_DATE = -1


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

initialize_date_lookup()
invalid_record = (False, 0, 0, 0, '0', '0', '0', '0', [], 0, 0, '0')

reporter = PigCounterHelper()
cnt_fhrv2 = 0
cnt_fhrv3 = 0
cnt_errors = 0

@outputSchema('t:tuple(valid:boolean, version:int, last:int, this:int, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, addons:bag{addon:tuple(vals:map[chararray])}, cpus:int, memory_in_mb:int, arch:chararray)')
def map(line):
    global cnt_errors, cnt_fhrv3, cnt_fhrv2

    try:
        x = loads(line)
        fhr_version = int(x['version'])
        gai = addons = sysinfo = None

        if fhr_version == 2:
            cnt_fhrv2 += 1
            reporter.incrCounter('stats', 'fhrv2', 1)
            gai = x['geckoAppInfo']
            addons = x['data']['last']['org.mozilla.addons.active']
            sysinfo = x['data']['last']['org.mozilla.sysinfo.sysinfo']
        elif fhr_version == 3:
            cnt_fhrv3 += 1
            reporter.incrCounter('stats', 'fhrv3', 1)
            gai = x['environments']['current']['geckoAppInfo']
            addons = x['environments']['current']['org.mozilla.addons.active']
            sysinfo = x['environments']['current']['org.mozilla.sysinfo.sysinfo']

        s = (True, fhr_version, date_to_int(x['lastPingDate']), date_to_int(x['thisPingDate'])) + get_gai_fields(gai) + (parse_addon_data(addons),) + parse_sysinfo(sysinfo)
        return s

    except:
        print >> sys.stderr, 'INVALID', line.tostring()
        reporter.incrCounter('stats', 'errors', 1)
        return invalid_record
