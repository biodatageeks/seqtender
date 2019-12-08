package org.biodatageeks

import htsjdk.samtools.SAMRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.biodatageeks.conf.InternalParams
import org.biodatageeks.formats.BDGBAMOutputFormat
import org.disq_bio.disq.{BaiWriteOption, HtsjdkReadsRdd, HtsjdkReadsRddStorage, SbiWriteOption}
import org.seqdoop.hadoop_bam.{BAMInputFormat, BAMOutputFormat, SAMRecordWritable}

class CustomRDDSAMRecordFunctions(rdd : RDD[SAMRecord]) {

  val logger = Logger.getLogger(getClass.getName)


  def saveAsBAMFile(path:String)(implicit sparkSession: SparkSession) = {

     val bamReadMethod =
        sparkSession
          .conf
          .get(InternalParams.BAM_IO_LIB)

        bamReadMethod match {
              case "hadoop-bam" => saveHaadopBAMAsBAMFile(path)
              case "disq" =>  saveDISQASBAMFile(path)
              case _ => throw new Exception (s"Uknown BAM read method: ${bamReadMethod}.")
        }
     }

  private def saveHaadopBAMAsBAMFile(path:String)(implicit sparkSession: SparkSession) = {

    logger.info("Using Hadoop-bam write method")

    val headerString = rdd.first().getHeader.getSAMString
    val nullPathString = "/tmp/null.bam"
    //Fix for Spark saveAsNewHadoopfile
    val hdfs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val nullPath = new org.apache.hadoop.fs.Path(nullPathString)
    if(hdfs.exists(nullPath)) hdfs.delete(nullPath,true)
    val conf = new Configuration()
    conf.set(InternalParams.HEADER_STRING, headerString)
    conf.set(InternalParams.PATH, path)
    conf.set(BAMOutputFormat.WRITE_SPLITTING_BAI, "true")
    conf.set(BAMOutputFormat.USE_INTEL_DEFLATER_PROPERTY, "false")
    conf.set(BAMInputFormat.TRAVERSE_UNPLACED_UNMAPPED_PROPERTY, "true")

    rdd
      .map(r => (NullWritable.get(),  {val record = new SAMRecordWritable();record.set(r);record}) )
      .saveAsNewAPIHadoopFile(nullPathString,classOf[NullWritable],classOf[SAMRecordWritable],classOf[BDGBAMOutputFormat[NullWritable,SAMRecordWritable]],conf)

  }

  private def saveDISQASBAMFile(path:String)(implicit sparkSession: SparkSession) = {

    logger.info("Using disq write method")

    val reads = new HtsjdkReadsRdd(rdd.first().getHeader,rdd)
    HtsjdkReadsRddStorage
      .makeDefault(sparkSession.sparkContext)
      .write(reads,
        path,
        BaiWriteOption.ENABLE,
        SbiWriteOption.ENABLE)
  }

}

object  CustomRDDSAMRecordFunctions {
  implicit def addCustomFunctions(rdd: RDD[SAMRecord]) = new CustomRDDSAMRecordFunctions(rdd)
}