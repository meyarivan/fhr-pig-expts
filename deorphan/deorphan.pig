-- Parquet storage configuration
SET parquet.page.size 1048576
SET parquet.block.size 536870912
SET parquet.compression snappy
SET parquet.enable.dictionary true

register 'extract.py' using jython as myfuncs;

-- Date used for calculating list of valid dates
SET start_date '$start_date';


fhr_with_meta = LOAD '$input' USING parquet.pig.ParquetLoader AS ( id:chararray, valid:boolean, version:int, last:int, this:int, moz_version:chararray, channel:chararray, os:chararray, xpcomabi:chararray, platform_build_id:chararray, app_build_id:chararray, checksum:chararray, checksum_all:chararray, chosen_date:chararray, ndays:int, ndays_with_session_data:int, country:chararray);

valid_groupable = FILTER fhr_with_meta BY (valid == True) AND (chosen_date != '');

grouped_by_checksum = GROUP valid_groupable BY checksum PARALLEL 512;

head_records = FOREACH grouped_by_checksum {
             c = COUNT(valid_groupable);
             d = ORDER valid_groupable BY ndays_with_session_data desc;
             GENERATE FLATTEN(myfuncs.get_head_records(d, c, '$max_rec_per_group'));
}

STORE head_records INTO '$output' USING PigStorage('\\t', '-schema');

