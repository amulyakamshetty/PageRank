//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//reducer to write outlinks and their initial rank values
public class InitialPageReduce extends Reducer<Text, Text, Text, Text> {
public void reduce(Text word, Iterable<Text> value, Context context)throws IOException, InterruptedException {
	
	Configuration config=context.getConfiguration(); //we will get the configuration
	double Length_of_doc=config.getDouble("length",0.0d); //we will get he document length from the driver class (N)
	Double Initial_Rank=1/Length_of_doc;                 //we set the  initial page rank value as 1/N as given in the pdf doc
	String output = "";
	output = output;
	
		for (Text count : value) {
			if(output.equals(""))
			output=output+count;     
			else
				output=output+";"+count;  //we will put delimiter btwn outlnks  
		
}if(!(word.equals("")))   //suppose word isnt null 
	context.write(word, new Text(output+"\t"+ String.valueOf(Initial_Rank)));   //we will write out the final output as outlink tab initial pagerank value
}

}

