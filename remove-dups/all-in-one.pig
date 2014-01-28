register 'fastjson-1.1.37.jar';
register 'datafu-1.2.0.jar';

register 'extract.py' using jython as myfuncs;

SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
-- SET pig.schematuple true; #TODO#
SET pig.exec.mapPartAgg true;

-- start_date determines earliest date (start_date - 180 days) used for checksums etc
SET start_date '$start_date';


fhr_data = LOAD '$input' using PigStorage() AS (id:bytearray, val:bytearray);

-- generate/extract metadata from each health report

fhr_with_meta = FOREACH fhr_data GENERATE id AS id, FLATTEN(myfuncs.process(val)) AS (valid:boolean, version:int, last:int, this:int, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, checksum:chararray, chosen_date:chararray, ndays:int), val;

-- STORE fhr_with_meta INTO '$output-wmeta' USING PigStorage();

valid_data = FILTER fhr_with_meta BY (valid == true) AND (version == 2) AND (SIZE(checksum) == 40);
grouped = GROUP valid_data BY (this, last, checksum);

-- count of entries per group (useful for debugging etc)

counts = FOREACH grouped GENERATE group, COUNT(valid_data);
counts_ord = ORDER counts BY $1 DESC;
STORE counts_ord INTO '$output-counts' USING PigStorage();

-- choose_valid_reports() returns valid (removes "duplicates", ...) reports from given group

final_set = FOREACH grouped GENERATE FLATTEN(myfuncs.choose_valid_reports(valid_data));
STORE final_set INTO '$output' USING PigStorage();

-- Continue processing :)
