
-- Copy of piggybank.jar bundled with given version of CDH
register 'piggybank.jar';

-- JSON parser
register 'fastjson-1.1.37.jar';

-- parquet dependencies built from source
register 'parquet-format-1.0.0.jar';
register 'parquet-encoding-1.2.9.jar';
register 'parquet-column-1.2.9.jar';
register 'parquet-common-1.2.9.jar';
register 'parquet-hadoop-1.2.9.jar';
register 'parquet-pig-1.2.9.jar';

-- elephantbird for rcfilestorage
register 'elephant-bird-core-4.3.jar';
register 'elephant-bird-pig-4.3.jar';
register 'elephant-bird-rcfile-4.3.jar';
register 'elephant-bird-hadoop-compat-4.3.jar';
register 'hive-serde-0.10.0-cdh4.3.0.jar';
register 'hive-common-0.10.0-cdh4.3.0.jar';
register 'hive-exec-0.10.0-cdh4.3.0.jar';

-- Python UDFs for parsing FHR
register 'extract.py' using jython as myfuncs;

-- parquet storage configuration
SET parquet.page.size 1048576 -- default. this is your min read/write unit.
SET parquet.block.size 134217728 -- default. your memory budget for buffering data
SET parquet.compression snappy -- or you can use none, gzip, snappy
SET parquet.enable.dictionary true

-- rcfile storage configuration
SET mapred.output.compression.type BLOCK;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

-- perf configuration
SET mapred.job.reuse.jvm.num.tasks 32;
SET pig.exec.mapPartAgg true;
SET pig.maxCombinedSplitSize 4194304;


-- processed: {id: bytearray,t::valid: boolean,t::version: int,t::last: int,t::this: int,t::moz_version: chararray,t::channel: chararray,t::os: chararray,t::xpcomabi: chararray,t::addons: {addon: (vals: map[chararray])},t::cpus: int,t::memory_in_mb: int,t::arch: chararray,data: bytearray}

processed = LOAD '$input' USING parquet.pig.ParquetLoader('t::valid:boolean, t::moz_version:chararray, t::channel:chararray, t::addons: {addon: (vals: map[chararray])}');

y = processed; --LIMIT processed 5000;

x = FOREACH y GENERATE valid, moz_version, channel, FLATTEN(addons) as addon:map[chararray];

z = FOREACH x GENERATE valid, moz_version, channel, addon#'name' AS name:chararray;

a = GROUP z BY (moz_version, channel, name);
b = FOREACH a GENERATE FLATTEN(group), COUNT(z) AS cnt:long;
c = ORDER b BY cnt;

STORE c INTO '$output' USING PigStorage();



