Roll your own "remove duplicates"/""orphans"" tool :)

[1] Export HBase data 
[2] Run all-in-one.pig
[3] Modify grouping criterion and/or selection criterion (extract.py/choose_valid_reports)
[4] GOTO 2

Ex:
# [1] ; Export all data with timestamp between 01-Jan-2014 00:00:00 UTC <-> 15-Jan-2014 00:00:00 UTC
pig -param output=fhr_20140101_20140114-raw -param min=$(( $(date -d 20140101 +%s) * 1000 )) -param max=$(( $(date -d 20140115 +%s) * 1000)) export.pig

# [2] 
pig -param input=fhr_20140101_20140114-raw/*snappy -param output=fhr_20140101_20140114-clean -param start_date=2014-01-15 all-in-one.pig

