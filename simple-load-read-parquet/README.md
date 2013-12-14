Sample scripts to export FHR data to HDFS

##### To mark export data with TS <= Today (00:00:00 UTC)
pig -param mintimestamp=0 -param maxtimestamp=$(( $(date -d $(date -d today +"%Y%m%d") +"%s") * 1000)) -param output=DEST_DIR export_fhr.pig 

##### To read exported data 
pig -param input=DEST_DIR/part* -param output=DEST_DIR_N  test_read.pig


