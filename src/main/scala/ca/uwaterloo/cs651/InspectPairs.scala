package ca.uwaterloo.cs651

import org.apache.log4j.{Logger}
import org.rogach.scallop.{ScallopConf}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}

class InspectPairsConf (arguments: Seq[String]) extends ScallopConf(arguments) {

    mainOptions = Seq(
        corpus_input_path,
        sentence_id_pairs_input_path,
        sentence_id_and_sentence_pairs_output_path
    )

    val corpus_input_path = opt[String](descr="", required=true)
    val sentence_id_pairs_input_path = opt[String](descr=".", required=true)
    val sentence_id_and_sentence_pairs_output_path = opt[String](descr=".", required=true) 

    verify()

}

object InspectPairs {

    val logger = Logger.getLogger(getClass().getName())

    def main (arguments: Array[String]) {        

        val conf = new InspectPairsConf(arguments)

        logger.info("--corpus_input_path: " + conf.corpus_input_path())
        logger.info("--sentence_id_pairs_input_path: " + conf.sentence_id_pairs_input_path())
        logger.info("--sentence_id_and_sentence_pairs_output_path: " + conf.sentence_id_and_sentence_pairs_output_path())

        val sparkConf = new SparkConf().setAppName("InspectPairs")
        val sparkContext = new SparkContext(sparkConf)

        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

        val corpusInputPath: Path = new Path(conf.corpus_input_path())

        if (!fileSystem.exists(corpusInputPath)) {
            logger.error("The corpus input path \"" + corpusInputPath.toString() + "\" does not exist")
            System.exit(1)
        }

        val sentenceIdPairsInputPath: Path = new Path(conf.sentence_id_pairs_input_path())
        
        if (!fileSystem.exists(sentenceIdPairsInputPath)) {
            logger.error("The sentence id pairs input path \"" + sentenceIdPairsInputPath.toString() + "\" does not exist")
            System.exit(1)
        }

        val sentenceIdAndSentencePairsOutputPath: Path = new Path(conf.sentence_id_and_sentence_pairs_output_path())

        fileSystem.delete(sentenceIdAndSentencePairsOutputPath, true) // If a file already exists under outputFilepath then delete it.

        val sentenceIdsOfInterest = (
            sparkContext.textFile(sentenceIdPairsInputPath.toString())
                        .flatMap(line => {

                            line.slice(1, line.length-1).split(',')

                        })
        ).collect().distinct

        val sentenceIdsOfInterestBroadcast = sparkContext.broadcast(sentenceIdsOfInterest)

        val senctenceIdsOfInterestToSentenceMappings = (
            sparkContext.textFile(corpusInputPath.toString())
                        .flatMap(line => {

                            val lineSplit: Array[String] = line.slice(1,line.length-1).split(',')

                            val document: String = lineSplit(1)
                            val documentId: String = lineSplit(0)
                            
                            val sentences: Array[String] = document.split('.')
                            sentences.zipWithIndex.flatMap(tuple => {

                                val sentence = tuple._1
                                val sentenceNumber = tuple._2
                                val sentenceId: String = documentId + "::" + sentenceNumber 

                                if (sentenceIdsOfInterestBroadcast.value.contains(sentenceId)) {
                                    List((sentenceId, sentence))
                                }
                                else {
                                    List()
                                }

                            })

                        })
        ).collect().toMap 
        
        val sentenceIdsOfInterestToSentenceMappingsBroadcast = sparkContext.broadcast(senctenceIdsOfInterestToSentenceMappings)

        val sentenceIdPairsWithSentences = (
            sparkContext.textFile(sentenceIdPairsInputPath.toString())
                        .map(line => {

                            val lineSplit: Array[String] = line.slice(1, line.length-1).split(',')

                            val sentenceIdA: String = lineSplit(0)
                            val sentenceIdB: String = lineSplit(1)

                            val sentenceA: String = sentenceIdsOfInterestToSentenceMappingsBroadcast.value.getOrElse(sentenceIdA, "")
                            val sentenceB: String = sentenceIdsOfInterestToSentenceMappingsBroadcast.value.getOrElse(sentenceIdB, "")

                            ((sentenceIdA, sentenceIdB), (sentenceA, sentenceB))

                        })
        )

        sentenceIdPairsWithSentences.saveAsTextFile(sentenceIdAndSentencePairsOutputPath.toString())

    }

}