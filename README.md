# MAP Reduce Implementation using Java

Same workers will act as map in first phase and reducers in second phase


Master
1) Establish Connection with all workers
2) Master coordinates with worker which phase to run and which are the intermediate files
3) Master tells worker which part of file to read
4) Chunk the input files such that each worker gets a part.
5) Send the start, end and wordLength to worker using sockets
6) Once reduce phases are done, join all the files to produce final output file

Worker
1) If map phase, then read segment of file and write the key values to intermediate files
2) If reduce phase, read all intermediate files and pick keys which they have to read
3) Use “hash(key) mod R” to check if that key is assigned to this particular reduce phase
4) Aggregate all such keys based on reduce function to get the final output