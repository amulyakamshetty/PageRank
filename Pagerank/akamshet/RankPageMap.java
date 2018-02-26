//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//map class for calculating pagerank values after iterations
public class RankPageMap extends Mapper<LongWritable, Text, FloatWritable,Text> {
	

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		try {
			context.getInputSplit();
			String ranks[]=lineText.toString().split("\t");  //we will convert text to string and split it on tab
			context.write(new FloatWritable(Float.parseFloat(ranks[2])), new Text(ranks[0])); //we will write each title and the value of page rank after doing the corresponding iterations
					} catch (Exception e) {
			e.printStackTrace();
		}
	}
}