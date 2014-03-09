-- Parquet storage configuration
SET parquet.page.size 1048576 -- default. this is your min read/write unit.
SET parquet.block.size 536870912 -- default. your memory budget for buffering data
SET parquet.compression snappy -- or you can use none, gzip, snappy
SET parquet.enable.dictionary true

register 'extract.py' using jython as myfuncs;

-- Date used for calculating list of valid dates etc.
SET start_date '$start_date';

fhr_data = LOAD '$input' using org.apache.pig.piggybank.storage.SequenceFileLoader() AS (id:chararray, val:chararray);
-- fhr_data = LOAD '$input' using PigStorage() AS (id:chararray, val:chararray);

-- generate/extract metadata from each health report

fhr_with_meta = FOREACH fhr_data GENERATE id AS id, FLATTEN(myfuncs.process(val)) AS (valid:boolean, version:int, last:int, this:int, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, platform_build_id:chararray, app_build_id:chararray, checksum:chararray, checksum_all:chararray, chosen_date:chararray, ndays:int, ndays_with_session_data:int, country:chararray);

STORE fhr_with_meta INTO '$output.meta' USING parquet.pig.ParquetStorer;
