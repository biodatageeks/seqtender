
DROP TABLE if EXISTS al_perf;

CREATE TABLE al_perf (
	test_id VARCHAR(50),
	piper VARCHAR(20),
	tool VARCHAR(20),
	format VARCHAR(20),
	time_finished TIMESTAMP DEFAULT current_timestamp,
	time_taken bigint,
	spark_mode varchar(20),
	cores integer,
	driver_mem integer,
	exec_mem integer,
	partitions_num integer,
	row_count bigint,
	split_size bigint,
	file_size bigint
	);