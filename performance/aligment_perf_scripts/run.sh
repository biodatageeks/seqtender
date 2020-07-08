. /data/work/projects/seqtender-perf/.profile

DEFAULT_SPLIT=134217728

SEQTENDER_JAR=/data/work/projects/bdg-seqtender/target/scala-2.11/bdg-seqtender-assembly-0.3-SNAPSHOT.jar
SEQTENDER=SEQTENDER


CANNOLI_JAR=/data/local/projects/git/cannoli0.9/cannoli/assembly/target/cannoli-assembly-spark2_2.11-0.10.0-SNAPSHOT.jar 
CANNOLI=CANNOLI

BWA_CONF=/data/work/projects/seqtender-perf/BwaConf.scala
BOWTIE2_CONF=/data/work/projects/seqtender-perf/Bowtie2Conf.scala

TEST_FILE=/data/work/projects/seqtender-perf/perf.scala
UTILS_FILE=/data/work/projects/seqtender-perf/utils.scala

TEST_MODE="$@"

for PIPER in $CANNOLI $SEQTENDER
do
	for CONF in $BOWTIE2_CONF $BWA_CONF 
	do
		for DIVISOR in {1..10}
		do
			SPLTI_SIZE=$((DEFAULT_SPLIT / DIVISOR))
			for EXEC_NUM in {40,30,20,10,9,8,7,6,5,4,3,2,1}
			do 
				kinit -kt /data/work/projects/seqtender-perf/bdg-perf.keytab  bdg-perf@CL.II.PW.EDU.PL 
				NOW=`date`
				echo "$NOW Running $PIPER with `basename $CONF`, divisor: $DIVISOR, exec number: $EXEC_NUM mode: yarn"  >> perf.log
				spark-shell  -v \
				--conf spark.sql.catalogImplementation=in-memory \
				--conf spark.hadoop.yarn.timeline-service.enabled=false  \
				--conf spark.dynamicAllocation.enabled=false \
				--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
				--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
				--master=yarn-client \
				--num-executors=$EXEC_NUM \
				--executor-memory=2g \
				--executor-cores=1 \
				--driver-memory=2g \
				--jars $SEQTENDER_JAR,$CANNOLI_JAR \
				-i <(echo val testMode=\"$TEST_MODE\") \
				-i <(echo val piper=\"$PIPER\") \
				-i $CONF \
				-i $UTILS_FILE \
				-i $TEST_FILE
			done

			for EXEC_NUM in {10,9,8,7,6,5,4,3,2,1}
			do 
				kinit -kt /data/work/projects/seqtender-perf/bdg-perf.keytab  bdg-perf@CL.II.PW.EDU.PL
				NOW=`date`
				echo "$NOW Running $PIPER with `basename $CONF`, divisor: $DIVISOR, exec number: $EXEC_NUM mode: local"  >> perf.log
				spark-shell  -v \
				--conf spark.sql.catalogImplementation=in-memory \
				--conf spark.dynamicAllocation.enabled=false \
				--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
				--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=$SPLTI_SIZE \
				--master=local[$EXEC_NUM] \
				--driver-memory=2g \
				--jars $SEQTENDER_JAR,$CANNOLI_JAR \
				-i <(echo val testMode=\"$TEST_MODE\") \
				-i <(echo val piper=\"$PIPER\") \
				-i $CONF \
				-i $UTILS_FILE \
				-i $TEST_FILE
			done
		done
	done
done

