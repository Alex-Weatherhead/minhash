
First run "python3 main.py" to get the "corpus.txt" file. Then move this corpus onto hadoop.

To run the MinHash algorithm:

spark-submit --class ca.uwaterloo.cs651.MinHash \
 --num-executors 4 --executor-cores 4 --executor-memory 24G \
 target/project-1.0.jar --input_path corpus.txt \
 --output_path sentenceIdPairs \
 --target_jaccard_similarity_of_pairs 0.60

Then, to combine each sentence id pair with its sentence pair:

spark-submit --class ca.uwaterloo.cs651.InspectPairs \
 --num-executors 4 --executor-cores 4 --executor-memory 24G \
 target/project-1.0.jar --corpus_input_path corpus.txt \
 --sentence_id_pairs_input_path sentenceIdPairs \
 --sentence_id_and_sentence_pairs_output_path sentenceIdAndSentencePairs

