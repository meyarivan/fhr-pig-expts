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


-- Parse and export all FHR records as rows with nested values for some of the columns

raw = LOAD 'hbase://metrics' USING 
    org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json', 
    '-loadKey=true -minTimestamp=$mintimestamp -maxTimestamp=$maxtimestamp -caching=1000 -limit=5 -lte=01') 
    AS (id:bytearray, data:bytearray);

-- ParquetStorer() dies with "can not get schema" if "raw_limited = LIMIT raw ..." is used
raw_limited = raw; 

processed = FOREACH raw_limited GENERATE id AS id,FLATTEN(myfuncs.map(data)),data;
STORE processed INTO '$output.pq' USING parquet.pig.ParquetStorer;
