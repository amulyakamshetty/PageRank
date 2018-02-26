//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//driver class which contains main and instantiates all the mapper and reducer classes
public class Driver extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		boolean success=TitleCount(args);
		success=InitialPageRank(args);
		
		for(int x=0;x<10;x++) //we wll first make folders for each of the ten iterations
		{
			String input1=args[1]+"FinalPageRank_"+x; //initializing strings in and out for  input and output
			String output1=args[1]+"FinalPageRank_"+(x+1); //naming the folders
			success=FinalPageRank(input1,output1);
			
		}
		String input2=args[1]+"FinalPageRank_10"; //string in is taken as the second argument and last folder
		String output2=args[1]+"SortedOutput";  //string out is taken as second argument and the sorted output
		success=SortPageRank(input2,output2);
		FileSystem hdfs = FileSystem.get(getConf());
		
		//Delete init outputpath if exists
		for(int x=0;x<= 10; x++){
		hdfs.delete(new Path(args[1]+"FinalPageRank_"+x), true);
		}
		hdfs.delete(new Path(args[1]+"Title_Count"), true);
		
		return success? 0:1 ;
	
	}
		
	
public boolean TitleCount(String[] args) throws Exception {           //first job is to count hte number of pages
	Job job1 = Job.getInstance(getConf(), "LinkGraph");
	job1.setJarByClass(this.getClass());
	// we will instantiate the map and reduce of that job
	job1.setMapperClass(CountPageMap.class);
	job1.setReducerClass(CountPageReduce.class);
	//we will instantiate the paths for input,output for the job 
	FileInputFormat.addInputPath(job1, new Path(args[0]));
	FileOutputFormat.setOutputPath(job1, new Path(args[1]
			+ "Title_Count"));
	//we will instantiate the output classes for map and reduce
	job1.setMapOutputKeyClass(Text.class);
	job1.setMapOutputValueClass(IntWritable.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(IntWritable.class);
	boolean success = job1.waitForCompletion(true); //waiting for job1 to complete to start the next job 
	
	return success;
}


	
public boolean InitialPageRank(String[] args) throws Exception {
	//setting the configuration object to contain input size which is obtained from job1
	Configuration conf = new Configuration();
	double num_of_titles = 0;
	Path path1= new Path(args[1]+ "Title_Count/part-r-00000" );
	FileSystem fsystem = FileSystem.get(conf);
	BufferedReader input = new BufferedReader(new InputStreamReader(fsystem.open(path1)));
	
	
	String str;
	while ((str = input.readLine()) != null) {
		String splitted[] = str.split("\t");  //splitting the string on tab and storing it in array
		num_of_titles = Double.parseDouble(splitted[1]);
		break;
	}
	input.close();
	conf.setDouble("length", num_of_titles);  ////we get the no. of input titles
	
	

	Job job2 = Job.getInstance(conf, "Initial PageRank"); //job2 is to calculate beginning pagerank values
	job2.setJarByClass(this.getClass());
	//we instantiate job2's map and reduce clases
	job2.setMapperClass(InitialPageMap.class);
	job2.setReducerClass(InitialPageReduce.class);
	//we set input,output paths for job2
	FileInputFormat.addInputPath(job2, new Path(args[0]));
	FileOutputFormat.setOutputPath(job2, new Path(args[1]
			+ "FinalPageRank_0"));
	//we set the respective output clases for map and reduce
	job2.setMapOutputKeyClass(Text.class);
	job2.setMapOutputValueClass(Text.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	boolean success = job2.waitForCompletion(true);
	return success;
}
public boolean FinalPageRank(String input, String output) throws Exception {  //job for calculating the page ranks after iterations
Job job3 = Job.getInstance(getConf(), "FinalPageRank");
job3.setJarByClass(this.getClass());
//set the Map and Reduce class for job3
job3.setMapperClass(FinalPageMap.class);
job3.setReducerClass(FinalPageReduce.class);
//set job3's input,output paths 
FileInputFormat.addInputPath(job3, new Path(input));
FileOutputFormat.setOutputPath(job3, new Path(output));
//set thhe respective outputclasses
job3.setMapOutputKeyClass(Text.class);
job3.setMapOutputValueClass(Text.class);

boolean success = job3.waitForCompletion(true);  //waiting for job3 to complete before starting job4
return success;
}
public boolean SortPageRank(String input, String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
	Job job4 = Job.getInstance(getConf(), "SortedOutput"); //job4 will sort thr pagerank in descending pattern
	job4.setJarByClass(this.getClass());
	//set job4's Map and Reduce class
	job4.setMapperClass(RankPageMap.class);
	job4.setReducerClass(RankPageReduce.class);
	job4.setSortComparatorClass(comp.class);
	// set input,output paths for job4
	FileInputFormat.addInputPath(job4, new Path(input));
	FileOutputFormat.setOutputPath(job4, new Path(output));
	//setting the respective output class
	job4.setMapOutputKeyClass(FloatWritable.class);
	job4.setMapOutputValueClass(Text.class);
	job4.setOutputKeyClass(Text.class);
	job4.setOutputValueClass(FloatWritable.class);
	boolean success = job4.waitForCompletion(true);
	return success;
}
	
	
public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Driver(), args);
	System.exit(res);
}
}

 class comp extends WritableComparator{ //we need Comparator to  sort descending pageranks
	protected comp(){
		super(FloatWritable.class,true);
		
	}
	public int compare(WritableComparable x,WritableComparable y){
		FloatWritable f1=(FloatWritable) x;
		FloatWritable f2=(FloatWritable) y;
		int result=f1.compareTo(f2);return result*-1;       
	}
}
