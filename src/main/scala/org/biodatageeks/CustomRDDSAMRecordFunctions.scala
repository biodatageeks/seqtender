package org.biodatageeks

import htsjdk.samtools.SAMFileHeader.SortOrder
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
import org.disq_bio.disq.{BaiWriteOption, HtsjdkReadsRdd, HtsjdkReadsRddStorage, ReadsFormatWriteOption, SbiWriteOption}
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
              case _ => throw new Exception (s"Uknown BAM write method: ${bamReadMethod}.")
        }
     }

  private def saveHaadopBAMAsBAMFile(path:String)(implicit sparkSession: SparkSession) = {

    logger.info("Using Hadoop-bam write method for saving SAMRecords")

    val nullPathString = "/tmp/null.bam"
    //Fix for Spark saveAsNewHadoopfile
    val hdfs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val nullPath = new org.apache.hadoop.fs.Path(nullPathString)
    if(hdfs.exists(nullPath)) hdfs.delete(nullPath,true)
    val conf = new Configuration()
    conf.set(InternalParams.PATH, path)

    lazy val sortedRDD = rdd.sortBy( r=> (r.getReferenceName, r.getStart))
    lazy val header = rdd.first().getHeader
    header.setSortOrder(SortOrder.coordinate)
    val headerString = header.getSAMString
    conf.set(InternalParams.HEADER_STRING, headerString)

    sortedRDD
      .map(r => (NullWritable.get(),  {val record = new SAMRecordWritable();record.set(r);record}) )
      .coalesce(1) //FIXME: does not work with multiple splits
      .saveAsNewAPIHadoopFile(nullPathString,classOf[NullWritable],classOf[SAMRecordWritable],classOf[BDGBAMOutputFormat[NullWritable,SAMRecordWritable]],conf)

  }

  private def saveDISQASBAMFile(path:String)(implicit sparkSession: SparkSession) = {

    logger.info("Using disq write method for saving SAMRecords")

    //sorting SAMRecords RDD and  updating header to coordinate order
    lazy val sortedRDD = rdd.sortBy( r=> (r.getReferenceName, r.getStart))
    lazy val header = rdd.first().getHeader
    header.setSortOrder(SortOrder.coordinate)
    lazy val reads = new HtsjdkReadsRdd(header,sortedRDD)
    val baiEnable = BaiWriteOption.ENABLE
    val sbiEnable = SbiWriteOption.ENABLE
    HtsjdkReadsRddStorage
      .makeDefault(sparkSession.sparkContext)
      .write(reads,
        path,
        ReadsFormatWriteOption.BAM,
        baiEnable,
        sbiEnable)
    logger.info (s"BAM file ${path} saved, with: BAI: ${baiEnable.toString}, SBI: ${sbiEnable.toString}.")
  }


}

object  CustomRDDSAMRecordFunctions {
  implicit def addCustomFunctions(rdd: RDD[SAMRecord]) = new CustomRDDSAMRecordFunctions(rdd)
}