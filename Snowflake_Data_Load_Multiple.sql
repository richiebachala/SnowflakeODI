!set variable_substitution=true;
alter session set query_tag = &querytag;
select current_timestamp as run_time;
put file://&data_file_directory/&data_file_name/*.&data_file_type.* @&stage_name AUTO_COMPRESS = TRUE PARALLEL=&putparalellism;
COPY INTO &table_name FROM @&stage_name PATTERN = '.*\/&data_file_name.&data_file_type.[0-9]+.gz' FILE_FORMAT = (FORMAT_NAME = '&fileformat' TIMESTAMP_FORMAT = '&timestampformat');
&purgestagefiles @&stage_name PATTERN = '.*\/&data_file_name.&data_file_type.[0-9]+.gz';
