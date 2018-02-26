//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class CountPageReduce extends Reducer<Text,IntWritable, Text, IntWritable> {

	
	//reducer to sum the number of pages 
public void reduce(Text title, Iterable<IntWritable> countvalue, Context context)throws IOException, InterruptedException {
	int sum=0; //we will initialize total to 0
	for (IntWritable value : countvalue) {
		sum=sum+value.get();    //we will add up all the pages
}context.write(title,new IntWritable(sum));    //we will write the total as value
}
}
