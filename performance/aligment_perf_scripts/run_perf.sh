. /tmp/.al_profile

DEFAULT_SPLIT=134217728
ASSEMBLY_JAR=/data/work/projects/bdg-seqtender/target/scala-2.11/bdg-seqtender-assembly-0.3-SNAPSHOT.jar
TEST_FILE=/tmp/perf_seqtender.scala

for DIVISOR in {1..10}
do
	SPLTI_SIZE=$((DEFAULT_SPLIT / DIVISOR))
	echo "Running with divisor $DIVISOR"

	## YARN mode

	echo "Running yarn mode"

	seq 1 1 10 | xargs  -i  spark-shell  \
	--conf spark.sql.catalogImplementation=in-memory \
	--conf spark.hadoop.yarn.timeline-service.enabled=false  \
	--conf spark.dynamicAllocation.enabled=false \
	--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
	--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
	--master=yarn-client \
	--num-executors={} \
	--executor-memory=2g \
	--executor-cores=1 \
	--driver-memory=4g \
	--jars $ASSEMBLY_JAR \
	-i $TEST_FILE



	seq 10 10 80 | xargs  -i  spark-shell  \
	--conf spark.sql.catalogImplementation=in-memory \
	--conf spark.hadoop.yarn.timeline-service.enabled=false  \
	--conf spark.dynamicAllocation.enabled=false \
	--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
	--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
	--master=yarn-client \
	--num-executors={} \
	--executor-memory=2g \
	--executor-cores=1 \
	--driver-memory=4g \
	--jars $ASSEMBLY_JAR \
	-i $TEST_FILE

	## local

	echo "Running local mode"

	seq 1 1 10 | xargs  -i  spark-shell  \
	--conf spark.sql.catalogImplementation=in-memory \
	--conf spark.dynamicAllocation.enabled=false \
	--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
	--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
	--master=local[{}] \
	--driver-memory=2g \
	-i $TEST_FILE \
	--jars $ASSEMBLY_JAR


	seq 1 1 10 | xargs  -i  spark-shell  \
	--conf spark.sql.catalogImplementation=in-memory \
	--conf spark.dynamicAllocation.enabled=false \
	--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
	--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
	--master=local[{}] \
	--driver-memory=4g \
	-i $TEST_FILE \
	--jars $ASSEMBLY_JAR

done
