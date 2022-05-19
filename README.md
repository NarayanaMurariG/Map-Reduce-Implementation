# MAP Reduce Implementation using Java

Using the Google's original map reduce research paper (https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf),
we have implemented the map reduce algorithm in a distributed way for the following use cases.
1) Word Count
2) URL Frequency Count
3) Reverse Web Link

We have implemented this system using the same architecture followed in the research paper.

## Design aspects and concepts used:
Multithreading and futures
IPC using Java Sockets
OOPS

## Assumptions made:
1) To actually implement the worker nodes in a distributed way, we would also need to implement a 
distributed file system, and to simplify the implementation, run all the worker nodes on the same 
local machine and the worker nodes run independent of each other.
2) Since reduce phase cannot begin before map phase is performed, we reuse the same worker nodes
used in map phase and reducers.
3) We are using only 3 worker nodes in this implementation, but it can also be
increased by changing configurations.

## Functionality of each class
### Master
1) Establish Connection with all workers
2) Master coordinates with worker which phase to run and which are the intermediate files
3) Master tells worker which part of file to read
4) Chunk the input files such that each worker gets a part.
5) Send the start, end and wordLength to worker using sockets
6) Once reduce phases are done, join all the files to produce final output file

### Worker
1) The workers are always in standby mode, waiting for instructions from master
2) Receives instruction from master as in to act as either map worker or reduce worker
3) If map phase, then read segment of file and write the key values to intermediate files
4) If reduce phase, read all intermediate files and pick keys which they have to read
5) Use “hash(key) mod R” to check if that key is assigned to this particular reduce phase
6) Aggregate all such keys based on reduce function to get the final output

## Statement of help
1) Need to start workers first. Each worker node takes the worker number as an argument.
2) Then start master process. Master process takes input file name and use case as arguments.

## Link for demo