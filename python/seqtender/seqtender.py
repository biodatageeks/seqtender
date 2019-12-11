from pyspark.sql import SparkSession
from typeguard import check_argument_types

class  SeqTenderAlignment:
    """
    Class for running reads alignment using industry standard tools.
    :param session: session in which the alignment will be run and parallelized
    :type session: SparkSession
    :param command: object mapping input parameters to the command to be run
    :type command: CommandBuilder

    """
    def __init__(self, session: SparkSession, readsPath, indexPath, tool,  interleaved = True, readGroupId = "", readGroup=""):
        """Creates a new SeqTenderAlignment.
        """
        self.session = session
        self.command = session._jvm.org.biodatageeks.alignment.CommandBuilder(readsPath, indexPath,tool,
                                                                              None, interleaved, readGroupId, readGroup)


    def pipe_reads(self):
        return self.session._jvm.org.biodatageeks.alignment.SeqTenderAlignment.pipeReads(self.command, self.session._jsparkSession)

    def save_reads(self, path, rdd):
        reads = self.session._jvm.org.biodatageeks.CustomRDDSAMRecordFunctions.addCustomFunctions(rdd)
        self.session._jsparkSession.conf().set("org.biodatageeks.seqtender.bamIOLib","disq")
        reads.saveAsBAMFile(path,self.session._jsparkSession )


class SeqTenderAnnotation:

    def __init__(self, session: SparkSession):
        """Creates a new SeqTenderAnnotation.
        """
        self.session = session

    def pipe_variants(self, path, command):
        return self.session._jvm.org.biodatageeks.SeqTenderVCF.pipeVCF(path, command, self.session._jsparkSession)


    def save_variants(self, path, rdd):
        variants = self.session._jvm.org.biodatageeks.CustomVariantContextFunctions.addCustomFunctions(rdd)
        variants.saveDISQAsVCFFile(path,self.session._jsparkSession )
