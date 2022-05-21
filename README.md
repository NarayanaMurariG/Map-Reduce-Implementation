# MAP Reduce Implementation using Java

Using the Google's original map reduce research paper (https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf),
we have implemented the map reduce algorithm in a distributed way for the following use cases.
1) Word Count
2) Distributed Grep
3) Reverse Web Link

We have implemented this system using the same architecture followed in the research paper.

## Word Count
It counts the number of occurrences for each and every word and computes them into three output files
Input is a document and output files are key value pairs of words and their count

## Reverse Web Link
It takes an input of sources and their targets and returns the key value pair, where key is the target and
value is the list of all the linked sources

## Distributed Grep
It takes a document and pattern as input and emits the line number and line containing the input pattern to
output files.

## Design aspects and concepts used:
Multithreading and futures
<br>
IPC using Java Sockets
<br>
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
5) Use hashcode and other math functions to check if that key is assigned to this particular reduce phase
6) Aggregate all such keys based on reduce function to get the final output

## Statement of help
1) Compile the code and classes.
2) Need to start workers first. Each worker node takes the worker number as an argument.
3) Move to the directory where the class files for project are generated or can also use run configurations if using any 
IDE like eclipse or IntelliJ
4) To start workers use below instruction or create run config
<br> Worker 1 : java mapreduce.Worker 1 8080
<br> Worker 2 : java mapreduce.Worker 2 8100 
<br>Worker 3 : java mapreduce.Worker 3 8120
5) Then start master process. Master process takes input file name and use case as arguments.
<br> Master : java mapreduce.MasterProcess
6) For Word Count use case (Takes inputfile, usecase as inputs)
<br> java mapreduce.Client hadoop.txt WORD_COUNT
7) For Reverse Web Link use case (Takes inputfile, usecase as inputs)
<br> java mapreduce.Client reverseWebLink.txt REVERSE_WEB_LINK
8) For Distributed Grep use case (Takes inputfile, usecase and pattern as inputs)
<br> java mapreduce.Client distributedGrep.txt DISTRIBUTED_GREP mailslot

## Screenshots of outputs and run configurations are stored in MapReduceOutputs directory 