
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

fhr_table = LOAD 'hbase://metrics' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json', '-loadKey true -caching 1000 -minTimestamp $min -maxTimestamp $max') AS (id:chararray, val:chararray);

STORE fhr_table INTO '$output' USING PigStorage();


