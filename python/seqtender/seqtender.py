from pyspark.sql import SparkSession
from typeguard import check_argument_types



class  SeqTenderAlignment:
    def __init__(self, session: SparkSession, readsPath, indexPath, tool,  interleaved = True):
        """Creates a new SeqTenderAlignment.
        """
        self.session = session
        self.command = session._jvm.org.biodatageeks.alignment.CommandBuilder(readsPath, indexPath,tool,
                                                                              None,interleaved, None, None  )


    def pipe_reads(self):
        return self.session._jvm.org.biodatageeks.alignment.SeqTenderAlignment.pipeReads(self.command, self.session._jsparkSession)

    def save_reads(self, path, rdd):
        reads = self.session._jvm.org.biodatageeks.CustomRDDSAMRecordFunctions.addCustomFunctions(rdd)
        self.session._jsparkSession.conf().set("org.biodatageeks.seqtender.bamIOLib","disq")
        reads.saveAsBAMFile(path,self.session._jsparkSession )
