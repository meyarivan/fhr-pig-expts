-- Parquet storage configuration
SET parquet.page.size 1048576 -- default. this is your min read/write unit.
SET parquet.block.size 536870912 -- default. your memory budget for buffering data
SET parquet.compression snappy -- or you can use none, gzip, snappy
SET parquet.enable.dictionary true

register 'extract_cc.py' using jython as myfuncs;

register 'datafu-1.2.0.jar';

DEFINE UnorderedPairs datafu.pig.bags.UnorderedPairs();

fhr_data = LOAD '$input' using org.apache.pig.piggybank.storage.SequenceFileLoader() AS (id:chararray, val:chararray);
-- fhr_data = LOAD '$input' using PigStorage() AS (id:chararray, val:chararray);

fhr_with_meta = FOREACH fhr_data GENERATE id AS id, FLATTEN(myfuncs.process(val));

fhr_valid = FILTER fhr_with_meta BY ((valid == True) AND (ndays_with_session_data > 0));

-- grouped = GROUP fhr_valid BY checksum;

-- linkable = FILTER grouped BY (COUNT(fhr_valid) > 1);

-- pairs = FOREACH linkable GENERATE UnorderedPairs(fhr_valid);

STORE fhr_valid INTO '$output' USING parquet.pig.ParquetStorer;


