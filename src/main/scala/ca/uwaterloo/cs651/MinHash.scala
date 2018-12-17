/**
 *
*/

package ca.uwaterloo.cs651

import org.apache.log4j.{Logger}
import org.rogach.scallop.{ScallopConf}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.Random

class MinHashConf (arguments: Seq[String]) extends ScallopConf(arguments) {

    val defaultTargetJaccardSimilarityOfPairs: Double = 0.9
    val defaultNumberOfBitsInHashValues: Int = 60
    val defaultNumberOfHashFunctions: Int = 20
    val defaultSeedsForHashFunctions: List[Long] = {
        val random = new Random(100003) // Seed a random with some large prime number.
        List.fill[Long](defaultNumberOfHashFunctions)(random.nextLong)
    }
    val defaultNumberOfBands: Int = 10
    val defaultSeedsForBands: List[Long] = {
        val random = new Random(103680) // // Seed a random with some large prime number.
        List.fill[Long](defaultNumberOfBands)(random.nextLong)
    }
    val defaultNumberOfHashFunctionsPerBand: Int = 10
    val defaultNumberOfCharactersPerShingle: Int = 12
    val defaultMinimumNumberOfShinglesToConsider: Int = 75
    val defaultMaximumNumberOfShinglesToConsider: Int = 600

    mainOptions = Seq(
        input_path,
        output_path,
        target_jaccard_similarity_of_pairs,
        number_of_bits_in_hash_values,
        number_of_hash_functions,
        seeds_for_hash_functions,
        number_of_bands,
        seeds_for_bands,
        number_of_hash_functions_per_band,
        number_of_characters_per_shingle,
        minimum_number_of_shingles_to_consider,
        maximum_number_of_shingles_to_consider
    )

    val input_path = opt[String](descr="The path to the text file containing the input corpus.", required=true)
    val output_path = opt[String](descr="The path to the directory under which the results will be written.", required=true) 
    val target_jaccard_similarity_of_pairs = opt[Double](descr="The jaccard similarity of pairs that will be used as a threshold for filtering out false positives. Must be between zero and one inclusive", default=Some(defaultTargetJaccardSimilarityOfPairs))
    val number_of_bits_in_hash_values = opt[Int](descr="", default=Some(defaultNumberOfBitsInHashValues))
    val number_of_hash_functions = opt[Int](descr="The number of hash functions to use.", default=Some(defaultNumberOfHashFunctions))
    val seeds_for_hash_functions = opt[List[Long]](descr="The list of the random seeds to use when creating each hash function. Must have length equal to --numberOfHashFunctions.", default=Some(defaultSeedsForHashFunctions))
    val number_of_bands = opt[Int](descr="The number of bands to use. Must be greater than or equal to one.", default=Some(defaultNumberOfBands))
    val seeds_for_bands = opt[List[Long]](descr="A list of the random seeds to use to create each band. Must have length equal to --numberOfBands.", default=Some(defaultSeedsForBands))
    val number_of_hash_functions_per_band = opt[Int](descr="The number of hash functions to use in each band. Must be greater than or equal to one.", default=Some(defaultNumberOfHashFunctionsPerBand))   
    val number_of_characters_per_shingle = opt[Int](descr="The number of characters per shingle. Must be greater than or equal to one.", default=Some(defaultNumberOfCharactersPerShingle)) 
    val minimum_number_of_shingles_to_consider = opt[Int](descr="The minimum number of shingles to consider when processing a given sentence. Must be greater than or equal to zero.", default=Some(defaultMinimumNumberOfShinglesToConsider))
    val maximum_number_of_shingles_to_consider = opt[Int](descr="The maximum number of shingles to consider when processing a given sentence. Must be greater than or equal to one.", default=Some(defaultMaximumNumberOfShinglesToConsider))

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
                     seedsForBands: List[Long]): List[List[Int]] = {
        
        return (
            for {
                i <- List.range(0, numberOfBands)
            } yield Random.shuffle(List.range(0, numberOfHashFunctions)).take(numberOfHashFunctionsPerBand)    
        )

    }

    def main (arguments: Array[String]) {        

        val conf = new MinHashConf(arguments)

        logger.info("--input_path: " + conf.input_path())
        logger.info("--output_path: " + conf.output_path())
        logger.info("--target_jaccard_similarity_of_pairs: " + conf.target_jaccard_similarity_of_pairs())
        logger.info("--number_of_bits_in_hash_values: " + conf.number_of_bits_in_hash_values())
        logger.info("--number_of_hash_functions: " + conf.number_of_hash_functions())
        logger.info("--seeds_for_hash_functions: " + conf.seeds_for_hash_functions())
        logger.info("--number_of_bands: " + conf.number_of_bands())
        logger.info("--seeds_for_bands: " + conf.seeds_for_bands())
        logger.info("--number_of_hash_functions_per_band: " + conf.number_of_hash_functions_per_band())
        logger.info("--number_of_characters_per_shingle: " + conf.number_of_characters_per_shingle())
        logger.info("--minimum_number_of_shingles_to_consider: " + conf.minimum_number_of_shingles_to_consider())
        logger.info("--maximum_number_of_shingles_to_consider: " + conf.maximum_number_of_shingles_to_consider())

        val targetJaccardSimilarityOfPairs: Double = conf.target_jaccard_similarity_of_pairs()

        if (targetJaccardSimilarityOfPairs < 0 || targetJaccardSimilarityOfPairs > 1) {
            logger.error("The target Jaccard Similarity of pairs must be between zero and one inclusive, but " + targetJaccardSimilarityOfPairs + " was given.")
            System.exit(1)
        }

        val numberOfBitsInHashValues: Int = conf.number_of_bits_in_hash_values()

        if (numberOfBitsInHashValues < 1) {
            logger.error("The number of bits in the hash values must be greater than or equal to one, but " + numberOfBitsInHashValues + " was given.")
            System.exit(1)
        }

        val numberOfHashFunctions: Int = conf.number_of_hash_functions()
        val seedsForHashFunctions: List[Long] = conf.seeds_for_hash_functions()

        if (numberOfHashFunctions < 1) {
            logger.error("The number of hash functions must be greater than or equal to one, but " + numberOfHashFunctions + " was given.")
            System.exit(1)
        }
        if (numberOfHashFunctions != seedsForHashFunctions.length) {
            logger.error("The number of hash function seeds must be equal to the number of hash functions, but " + seedsForHashFunctions.length + " and " +  numberOfHashFunctions + " were given respectively.")
            System.exit(1)
        }

        val numberOfBands: Int = conf.number_of_bands()
        val seedsForBands: List[Long] = conf.seeds_for_bands()

        if (numberOfBands < 1) {
            logger.error("The number of bands must be greater than or equal to one, but " + numberOfBands + " was given.")
            System.exit(1)
        }
        if (numberOfBands != seedsForBands.length) {
            logger.error("The number of band seeds must be equal to the number of bands, but " + seedsForBands.length + " and " +  numberOfBands + " were given respectively.")
            System.exit(1)
        }

        val numberOfHashFunctionsPerBand: Int = conf.number_of_hash_functions_per_band()

        if (numberOfHashFunctionsPerBand < 1) {
            logger.error("The number of hash functions per band must be greater than or equal to one, but " + numberOfHashFunctionsPerBand + " was given.")
            System.exit(1)
        }
        if (numberOfHashFunctionsPerBand > numberOfHashFunctions) {
            logger.error("The number of hash functions per band may not exceed the number of hash functions, but " + numberOfHashFunctionsPerBand + " and " + numberOfHashFunctions + " were given respectively.")
            System.exit(1)
        }

        val numberOfCharactersPerShingle: Int = conf.number_of_characters_per_shingle()

        if (numberOfCharactersPerShingle < 1) {
            logger.error("The number of characters per shingle must be greater than or equal to one, but " + numberOfCharactersPerShingle + " was given.")
            System.exit(1)
        }

        val minimumNumberOfShinglesToConsider: Int = conf.minimum_number_of_shingles_to_consider()

        if (minimumNumberOfShinglesToConsider < 0) {
            logger.error("The minimum number of shingles to consider must be greater than or equal to zero, but " + minimumNumberOfShinglesToConsider + " was given.")
            System.exit(1)
        }

        val maximumNumberOfShinglesToConsider: Int = conf.maximum_number_of_shingles_to_consider()

        if (maximumNumberOfShinglesToConsider < 1) {
            logger.error("The maximum number of shingles to consider must be greater than or equal to one, but " + maximumNumberOfShinglesToConsider + " was given.")
            System.exit(1)
        }

        val sparkConf = new SparkConf().setAppName("MinHash")
        val sparkContext = new SparkContext(sparkConf)
        
        val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

        val inputPath: Path = new Path(conf.input_path())
        
        if (!fileSystem.exists(inputPath)) {
            logger.error("The input file \"" + inputPath.toString() + "\" does not exist")
            System.exit(1)
        }

        val outputPath: Path = new Path(conf.output_path())

        fileSystem.delete(outputPath, true) // If a file already exists under outputPath then delete it.

        val hashFunctions = new MultiplyShiftHash(
            numberOfBitsInHashValues,
            seedsForHashFunctions.toArray
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

        val nearDuplicatePairsOfSentences = (
            sparkContext.textFile(inputPath.toString())
                        .flatMap(line => {
                            // Determine each signature for each sentence.

                            val lineSplit: Array[String] = line.slice(1,line.length-1).split(',')
                            
                            val document: String = if (lineSplit.length == 2) lineSplit(1) else ""
                            val documentId: String = lineSplit(0)
                            
                            val sentences: Array[String] = document.split('.')
                            sentences.zipWithIndex.flatMap(tuple => {

                                val sentence = tuple._1
                                val sentenceNumber = tuple._2
                                val sentenceId: String = documentId + "::" + sentenceNumber 
                    
                                // This calculation only holds for stride of 1, which is the only case we allow for.
                                // By doing this simple calculation, we save on a lot of unecessary object creation 
                                // because in the first case, the shingles themselves aren't needed, and in the second 
                                // case, we would have to create the iterator twice (once to count length and once to use).
                                val numberOfShingles: Int = sentence.length - (numberOfCharactersPerShingle - 1)

                                if (numberOfShingles < minimumNumberOfShinglesToConsiderBroadcast.value ||
                                    numberOfShingles > maximumNumberOfShinglesToConsiderBroadcast.value) {

                                    List() // We ignore this sentence, as it either subceeds or exceeds the permissable number of shingles. 

                                }
                                else {

                                    val minHashes = Array.fill[Long](numberOfHashFunctionsBroadcast.value)(Long.MaxValue)

                                    val shingles: Iterator[String] = sentence.sliding(numberOfCharactersPerShingle, 1)

                                    for (shingle <- shingles) {
                                        val hashValuesOfShingle: Array[Long] = hashFunctionsBroadcast.value.hashStr(shingle)
                                        for (hf <- 0 to numberOfHashFunctionsBroadcast.value - 1) {
                                            if (hashValuesOfShingle(hf) < minHashes(hf)) {
                                                minHashes(hf) = hashValuesOfShingle(hf)
                                            }
                                        }
                                    }

                                    val minHashesForBands: List[List[Long]] = (
                                        for {
                                            b <- List.range(0, numberOfBandsBroadcast.value)
                                        }
                                        yield { 
                                            for {
                                                hf <- List.range(0, numberOfHashFunctionsBroadcast.value)
                                                if (bandsBroadcast.value(b).contains(hf))
                                            }
                                            yield {
                                                minHashes(hf) 
                                            }     
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

                            })

                        })
                        .groupByKey()
                        .filter(tuple => {
                            // Filters out signatures that belong to only one sentence.

                            val iterable: Iterable[(String, Array[Long])] = tuple._2 

                            iterable.size > 1 

                        })
                        .flatMap(tuple => {
                            // Finds all candidate pairs for the given signature.

                            val signature: String = tuple._1
                            val iterable: Iterable[(String, Array[Long])] = tuple._2

                            for {
                                (sentenceIdA, minHashesA) <- iterable
                                (sentenceIdB, minHashesB) <- iterable
                                if (sentenceIdA.split("::")(0) != sentenceIdB.split("::")(0))
                            }
                            yield {

                                // It is important to maintain some sort of consistent ordering
                                // so that duplicates candidate pairs can be easily filtered out.
                                if (sentenceIdA <= sentenceIdB) {
                                    logger.debug("Candidate pair: " + sentenceIdA + ", " + sentenceIdB)
                                    ((sentenceIdA, sentenceIdB), (minHashesA, minHashesB))
                                }
                                else{
                                    logger.debug("Candidate pair: " + sentenceIdB + ", " + sentenceIdA)
                                    ((sentenceIdB, sentenceIdA), (minHashesB, minHashesA))
                                }

                            }

                        })
                        .groupByKey()
                        .map(tuple => {
                            // Filters out duplicate sentenceId candidate pairs.      

                            // Since a groupByKey() was just done, all the pairs in the
                            // iterable should be the same, so simply take the first.
                            (tuple._1, tuple._2.head) 

                        })
                        
                        .filter(tuple => {
                            // Filters out false positives.

                            val minHashesA: Array[Long] = tuple._2._1
                            val minHashesB: Array[Long] = tuple._2._2

                            var estimatedJaccardSimilarityOfPair: Double = 0
                                    
                            for (hfpb <- 0 to numberOfHashFunctionsPerBandBroadcast.value - 1) {
                                estimatedJaccardSimilarityOfPair += hfpb.toDouble / numberOfHashFunctionsPerBandBroadcast.value
                            }

                            logger.info("Estimated vs target: " + estimatedJaccardSimilarityOfPair + ", " + targetJaccardSimilarityOfPairsBroadcast.value)

                            estimatedJaccardSimilarityOfPair >= targetJaccardSimilarityOfPairsBroadcast.value

                        })
                        .map(tuple => {
                            // Drops the minHashes from the tuples.

                            tuple._1

                        })  
                        
        )

        nearDuplicatePairsOfSentences.saveAsTextFile(outputPath.toString())

    }

}