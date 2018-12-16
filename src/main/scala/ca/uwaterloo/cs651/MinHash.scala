/**
 *
*/

package cs.uwaterloo.cs651

import org.apache.log4j.{Logger}
import org.rogach.scallop.{ScallopConf}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.Random

class MinHashConf (arguments: Seq[String]) extends ScallopConf(arguments) {

    mainOptions = Seq(
        inputFilepath,
        outputFilepath,
        numberOfReducers,
        targetJaccardSimilarityOfPairs,
        numberOfBitsInHashValues,
        numberOfHashFunctions,
        seedsForHashFunctions,
        numberOfBands,
        seedsForBands,
        numberOfHashFunctionsPerBand,
        numberOfCharactersPerShingle,
        minimumNumberOfShinglesToConsider,
        maximumNumberOfShinglesToConsider
    )

    val defaultNumberOfReducers: Int = 1
    val defaultTargetJaccardSimilarityOfPairs: Float = 0.9
    val defaultNumberOfBitsInHashValues: Int = 60
    val defaultNumberOfHashFunctions: Int = 20
    val defaultSeedsForHashFunctions: List[Long] = {
        val random = Random(100003) // Seed a random with some large prime number.
        Array.fill[Long](defaultNumberOfHashFunctions)(random.nextLong)
    }
    val defaultNumberOfBands: Int = 10
    val defaultSeedsForBands: List[Long] = {
        val random = Random(103680) // // Seed a random with some large prime number.
        Array.fill[Long](defaultNumberOfBands)(random.nextLong)
    }
    val defaultNumberOfHashFunctionsPerBand: Int = 10
    val defaultNumberOfCharactersPerShingle: Int = 12
    val defaultMinimumNumberOfShinglesToConsider: Int = 75
    val defaultMaximumNumberOfShinglesToConsider: Int = 600

    val inputFilepath = opt[String](descr="The path to the text file containing the input corpus.", required=True)
    val outputFilepath = opt[String](descr="The path to the text file in which the results will be written.", required=True) 

    val numberOfReducers = opt[Int](descr="The number of reducers for Spark to use.", default=Some(defaultNumberOfReducers))

    val targetJaccardSimilarityOfPairs = opt[Float](descr="The jaccard similarity of pairs that will be used as a threshold for filtering out false positives. Must be between zero and one inclusive", default=Some(defaultTargetJaccardSimilarityOfPairs))
    val numberOfBitsInHashValues = opt[Int](descr="", default=Some(defaultNumberOfBitsInHashValues))
    val numberOfHashFunctions = opt[Int](descr="The number of hash functions to use.", default=Some(defaultNumberOfHashFunctions))
    val seedsForHashFunctions = opt[List[Long]](descr="The list of the random seeds to use when creating each hash function. Must have length equal to --numberOfHashFunctions.", default=Some(defaultSeedsForHashFunctions))
    val numberOfBands = opt[Int](descr="The number of bands to use. Must be greater than or equal to one.", default=Some(defaultNumberOfBands))
    val seedsForBands = opt[List[Int]](descr="A list of the random seeds to use to create each band. Must have length equal to --numberOfBands.", default=Some(defaultSeedsForBands))
    val numberOfHashFunctionsPerBand = opt[Int](descr="The number of hash functions to use in each band. Must be greater than or equal to one.", default=Some(defaultNumberOfHashFunctionsPerBand))   
    val numberOfCharactersPerShingle = opt[Int](descr="The number of characters per shingle. Must be greater than or equal to one.", default=Some(defaultNumberOfCharactersPerShingle)) 
    val minimumNumberOfShinglesToConsider = opt[Int](descr="The minimum number of shingles to consider when processing a given sentence. Must be greater than or equal to zero.", default=Some(defaultMinimumNumberOfShinglesToConsider))
    val maximumNumberOfShinglesToConsider = opt[Int](descr="The maximum number of shingles to consider when processing a given sentence. Must be greater than or equal to one.", default=Some(defaultMaximumNumberOfShinglesToConsider))

    verify()

}

object MinHash {

    val logger = Logger.getLogger(getClass().getName())

    /**
     * 
    */
    def createBands (numberOfHashFunctions: Int,
                     numberOfBands: Int,
                     numberOfHashFunctionsPerBand: Int,
                     seedsForBands: List[Int]): List[List[Int]] = {
        
        return (
            for {
                i <- List.range(0, numberOfBands)
            } 
            yield {
                Random.shuffle(List.range(0, numberOfHashFunctions)).take(numberOfHashFunctionsPerBand)
            }
        )

    }

    def main (arguments: Array[String]) {

        val conf = MinHashConf(arguments)

        logger.info("inputFilepath: " + conf.inputFilepath())
        logger.info("outputFilepath: " + conf.outputFilepath())
        logger.info("--targetJaccardSimilarityOfPairs: " + conf.targetJaccardSimilarityOfPairs())
        logger.info("--numberOfReducers: " + conf.numberOfReducers())
        logger.info("--numberOfBitsInHashValues: " + conf.numberOfBitsInHashValues())
        logger.info("--numberOfHashFunctions: " + conf.numberOfHashFunctions())
        logger.info("--seedsForHashFunctions: " + conf.seedsForHashFunctions())
        logger.info("--numberOfBands: " + conf.numberOfBands())
        logger.info("--seedsForBands: " + conf.seedsForBands())
        logger.info("--numberOfHashFunctionsPerBand: " + conf.numberOfHashFunctionsPerBand())
        logger.info("--numberOfCharactersPerShingle: " + conf.numberOfCharactersPerShingle())
        logger.info("--minimumNumberOfShinglesToConsider: " + conf.minimumNumberOfShinglesToConsider())
        logger.info("--maximumNumberOfShinglesToConsider: " + conf.maximumNumberOfShinglesToConsider())

        val targetJaccardSimilarityOfPairs: Float = conf.targetJaccardSimilarityOfPairs()

        if (targetJaccardSimilarityOfPairs < 0 || targetJaccardSimilarityOfPairs > 1) {
            logger.error("The number of hash functions must be greater than or equal to one, but " + numberOfHashFunctions + " was given.")
            System.exit(1)
        }

        val numberOfHashFunctions: Int = conf.numberOfHashFunctions().getOrElse(0)
        val seedsForHashFunctions: List[Long] = conf.seedsForHashFunctions().getOrElse(List())

        if (numberOfHashFunctions < 1) {
            logger.error("The number of hash functions must be greater than or equal to one, but " + numberOfHashFunctions + " was given.")
            System.exit(1)
        }
        if (numberOfHashFunctions != seedsForHashFunctions.length) {
            logger.error("The number of hash function seeds must be equal to the number of hash functions, but " + seedsForHashFunctions.length + " and " +  numberOfHashFunctions + " were given respectively.")
            System.exit(1)
        }

        val numberOFBands: Int = conf.numberOfBands().getOrElse(0)
        val seedsForBands: List[Int] = conf.seedsForBands().getOrElse(List())

        if (numberOfBands < 1) {
            logger.error("The number of bands must be greater than or equal to one, but " + numberOfBands + " was given.")
            System.exit(1)
        }
        if (numberOfBands != seedsForBands.length) {
            logger.error("The number of band seeds must be equal to the number of bands, but " + seedsForBands.length + " and " +  numberOfBands + " were given respectively.")
            System.exit(1)
        }

        val numberOfHashFunctionsPerBand: Int = conf.numberOfHashFunctionsPerBand().getOrElse(0)

        if (numberOfHashFunctionsPerBand < 1) {
            logger.error("The number of hash functions per band must be greater than or equal to one, but " + numberOfHashFunctionsPerBand + " was given.")
            System.exit(1)
        }
        if (numberOfHashFunctionsPerBand > numberOfHashFunctions) {
            logger.error("The number of hash functions per band may not exceed the number of hash functions, but " + numberOfHashFunctionsPerBand + " and " + numberOfHashFunctions + " were given respectively.")
            System.exit(1)
        }

        val numberOfCharactersPerShingle: Int = conf.numberOfCharactersPerShingle().getOrElse(0)

        if (numberOfCharactersPerShingle < 1) {
            logger.error("The number of characters per shingle must be greater than or equal to one, but " + numberOfCharactersPerShingle + " was given.")
            System.exit(1)
        }

        val minimumNumberOfShinglesToConsider: Int = conf.minimumNumberOfShinglesToConsider().getOrElse(0)

        if (minimumNumberOfShinglesToConsider < 0) {
            logger.error("The minimum number of shingles to consider must be greater than or equal to zero, but " + minimumNumberOfShinglesToConsider + " was given.")
            System.exit(1)
        }

        val maximumNumberOfShinglesToConsider: Int = conf.maximumNumberOfShinglesToConsider().getOrElse(0)

        if (maximumNumberOfShinglesToConsider < 1) {
            logger.error("The maximum number of shingles to consider must be greater than or equal to one, but " + maximumNumberOfShinglesToConsider + " was given.")
            System.exit(1)
        }

        val sparkConf = new SparkConf().setAppName("MinHash")
        val sparkContext = new SparkContext(sparkConf)
        
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

        val inputFilepath: String = Path(conf.inputFilepath())
        
        if (!fileSystem.exists(inputFilepath)) {
            logger.error("The input file \"" + inputFilepath.toUri() + "\" does not exist")
            System.exit(1)
        }

        val outputFilepath: String = Path(conf.outputFilepath())

        fileSystem.delete(outputFilepath, true) // If a file already exists under outputFilepath then delete it.

        val hashFunctions = new MultiplyShiftHash(
            numberOfBitsInHashValues,
            seedsForHashFunctions
        )

        val bands = createBands(
            numberOfHashFunctions,
            numberOfBands,
            numberOfHashFunctionsPerBand,
            seedsForBands
        )

        val targetJaccardSimilarityOfPairsBroadcast = sparkContext.broadcast(targetJaccardSimilarityOfPairs)
        val numberOfBandsBroadcast = sparkContext.broadcast(numberOfBands)
        val numberOfHashFunctionsBroadcast = sparkContext.broadcast(numberOfHashFunctions)
        val numberOfHashFunctionsPerBandBroadcast = sparkContext.broadcast(numberOfHashFunctions)
        val minimumNumberOfShinglesToConsiderBroadcast = sparkContext.broadcast(minimumNumberOfShinglesToConsider)
        val maximumNumberOfShinglesToConsiderBroadcast = sparkContext.broadcast(maximumNumberOfShinglesToConsider)
        val hashFunctionsBroadcast = sparkContext.broadcast(hashFunctions)
        val bandsBroadcast = sparkContext.broadcast(bands)

        val outputs = (
            sparkContext.textFile(inputFilepath.toUri())
                        .flatMap(line: String => {

                            val lineSplit: Array[String] = line.split(",")

                            val document: String = lineSplit(1)
                            val doumentId: String = lineSplit(0)
                                    
                            val sentences: Array[String] = document.split(".")
                            sentences.zipWithIndex.flatMap(tuple: (String, Int) => {

                                val sentence = tuple._1
                                val sentenceNumber = tuple._2
                                val sentenceId: String = documentId + "::" + sentenceNumber 

                                val shingles: Iterator[String] = (
                                    for {
                                        shingle <- sentence.sliding(numberOfCharactersPerShingle, 1)
                                    }
                                    yield {
                                        shingle
                                    }
                                )                 

                                if (shingles.length < minimumNumberOfShinglesToConsiderBroadcast.value ||
                                    shingles.length > maximumNumberOfShinglesToConsiderBroadcast.value) {

                                    List() // We ignore this sentence, as it either subceeds or exceeds the permissable number of shingles. 

                                }
                                else {

                                    val minHashes = Array.fill[Long](numberOfHashFunctions.value)(Long.MaxValue)

                                    for (shingle <- shingles) {
                                        val hashValuesOfShingle: Array[Long] = hashFunctionsBroadcast.value.hashStr(shingle)
                                        for (hf <- 0 to numberOfHashFunctionsBroadcast.value) {
                                            if (hashValuesOfShingle(hf) < minHashes(hf)) {
                                                minHashes(hf) = hashValuesOfShingle(hf)
                                            }
                                        }
                                    }

                                    minHashesForBands: List[List[String]] =  (
                                        for {
                                            b <- List.range(0, numberOfBandsBroadcast.value)
                                        }
                                        yield {
                                            (
                                                for {
                                                    hf <- List.range(0, numberOfHashFunctionsBroadcast.value)
                                                    if (bandsBroadcast.value(b).contains(hf))
                                                }
                                                yield {
                                                    minHashes(hf)
                                                }
                                            )
                                        }
                                    )

                                    // Use the MinHash values for each band to create the various signatures for the sentence.
                                    for {
                                        minHashesForBand <- minHashesForBands
                                    }
                                    yield {
                                        (minHashesForBand.toString, (sentenceId, minHashes))
                                    }

                                }

                            }

                        })
                        .groupByKey()
                        .filter(tuple: (String, Iterable[(String, List[Int])]) => {
                            // Filters out signatures that belong to only one sentence.

                            val iterable: Iterable[(String, List[Int])] = tuple._2

                            iterable.length > 1 

                        })
                        .flatMap(tuple: (String, Iterable[(String, List[Int])]) => {
                             // Finds all candidate pairs for the given signature.

                            val signature: String = tuple._1
                            val iterable: Iterable[(String, List[Int])] = tuple._2

                            for {
                                (sentenceIdA, minHashesA) <- iterable
                            }
                            yield {
                                for {
                                    (sentenceIdB, minHashesB) <- iterable
                                    if (sentenceIdB != sentenceIdB)
                                }
                                yield {
                                    // It is important to maintain some sort of consistent ordering
                                    // so that duplicates candidate pairs can be easily filtered out.
                                    if (setenceIdA <= sentenceIdB) {
                                        ((setenceIdA, sentenceIdB), (minHashesA, minHashesB))
                                    }
                                    else{
                                        ((sentenceIdB, sentenceIdA), (minHashesB, minHashesA))
                                    }
                                }
                            }

                        })
                        .groupByKey() 
                        .mapPartitions(tuple: ((String, String), Iterable[(List[Int], List[Int])]) => {
                            // Filters out duplicate sentenceId candidate pairs.
                                    
                            // Since a groupByKey() was just done, all the pairs in the
                            // iterable should be the same, so simply take the first.
                            (tuple._1, tuple._2.take(1)) 

                        })
                        .filter(tuple: ((String, String), (List[Int], List[Int])) => {
                            // Filters out false positives.

                            val minHashesA = tuple._2._1
                            val minHashesB = tuple._2._2

                            var estimatedJaccardSimilarityOfPair = 0
                                    
                            for (hfpb <- 0 to numberOfHashFunctionsPerBandBroadcast.value) {
                                estimatedJaccardSimilarityOfPair += hfpb / numberOfHashFunctionsPerBandBroadcast.value
                            }

                            estimatedJaccardSimilarityOfPair >= targetJaccardSimilarityOfPairsBroadcast.value

                        })
                        .map(tuple: ((String, String), (List[Int], List[Int])) => {
                            // Drops the minHashes from the tuples.

                            tuple._1

                        })  

        )

        output.saveAsTextFile(outputFilepath.toUri())

    }

}