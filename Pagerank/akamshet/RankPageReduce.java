//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


//reducer to write values to output from the mapper
public class RankPageReduce extends Reducer<FloatWritable, Text, Text, FloatWritable> {

	public void reduce(FloatWritable value, Iterable<Text> lineText, Context context)
			throws IOException, InterruptedException {

			for(Text word:lineText)	//we will iterate through the lines
		context.write(word,value);   //we will write values to the output from the mapper
	}
}