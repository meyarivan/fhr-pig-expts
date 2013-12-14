

import os, sys

import com.alibaba.fastjson.JSON as JSON
import java.util.Map as Map
from datetime import datetime, timedelta

#import org.apache.pig.tools.pigstats.PigStatusReporter

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

    return (moz_version, platform_version, os, xpcomabi)

    
initialize_date_lookup()

invalid_record = (False, 0, 0, 0, '0', '0', '0', '0')

@outputSchema('t:tuple(valid:boolean, version:int, last:int, this:int, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray)')
def map(line):
    try:
        x = loads(line)
        fhr_version = int(x['version'])
        gai = None

        if fhr_version == 2:
            gai = x['geckoAppInfo']
        elif fhr_version == 3:
            gai = x['environments']['current']['geckoAppInfo']

        return (True, fhr_version, date_to_int(x['lastPingDate']), date_to_int(x['thisPingDate'])) + get_gai_fields(gai)
    except:
        print >> sys.stderr, 'INVALID', line
        return invalid_record
