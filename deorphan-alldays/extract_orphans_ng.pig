-- Parquet storage configuration
SET parquet.page.size 1048576 -- default. this is your min read/write unit.
SET parquet.block.size 536870912 -- default. your memory budget for buffering data
SET parquet.compression snappy -- or you can use none, gzip, snappy
SET parquet.enable.dictionary true

register 'extract_cc.py' using jython as myfuncs;

register 'datafu-1.2.0.jar';
define Enumerate datafu.pig.bags.Enumerate('1');

fhr_valid = LOAD '$input' using parquet.pig.ParquetLoader AS (id:chararray, valid:boolean, version:int, lastping:chararray, thisping:chararray, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, platform_build_id:chararray, app_build_id:chararray, checksum:chararray, ndays:int, ndays_with_session_data:int, country:chararray, start_day:int, active_ticks:int, total_time:int, main:int, first_paint:int, session_restored:int);

grouped = GROUP fhr_valid BY checksum PARALLEL 3072;

linkable = FILTER grouped BY (COUNT(fhr_valid) > 1L);

orphans_hr = FOREACH linkable GENERATE FLATTEN(myfuncs.get_hr(fhr_valid));
orphans_all = FOREACH linkable GENERATE FLATTEN(fhr_valid);

j = UNION orphans_hr, orphans_all;
k = GROUP j BY (checksum, id);
l = FILTER k BY (COUNT(j) == 1L);

m = FOREACH l GENERATE FLATTEN(j);
orphans = FOREACH m GENERATE $0..$10,$12..;
orphans_uniq = DISTINCT orphans;

STORE orphans_uniq INTO '$output' USING PigStorage();
