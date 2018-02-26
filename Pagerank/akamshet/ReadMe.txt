//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

For the program to run ensure cloudera is installed in your computer and is being accessed using virtual machine that has enough memory access given for proper funtionality.
The steps for compiling and executing the code is given below:

Step-1:
create a folder "pagerank" in /user/cloudera/ using the command
hadoop fs -mkdir /user/cloudera/pagerank

Step-2:
create a folder "input" in /user/cloudera/pagerank using the following command
hadoop fs -mkdir /user/cloudera/pagerank/input

Step-3:
Put the input file wiki-micro.txt from your current directory to the input directory in Hadoop file system using the following command:
hadoop fs -put wiki-micro.txt /user/cloudera/pagerank/input

Step-4:
The following commands are executed one by one, which involves the deletion of output files and then doing all the Map-Reduce jobs

hadoop fs -rm -r /user/cloudera/pagerank/outp*
mkdir -p build
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint
jar -cvf pagerank.jar -C build/ .
hadoop jar pagerank.jar Driver /user/cloudera/pagerank/input/wiki-micro.txt /user/cloudera/pagerank/output


Step-5:
Now, we get the output file from the hadoop file system into the current directory and store into a text file
hadoop fs -cat /user/cloudera/pagerank/output*/* > outputWiki.txt

The generated output with the titles of the web pages with their corresponding pageranks are stored in outputWiki.txt
