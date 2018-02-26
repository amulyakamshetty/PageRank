//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

import java.io.File;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


//this mapper will count the number of pages in the file which is equal to the number of lines
public class CountPageMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		context.getInputSplit();
        String get_title[] = new String[2]; //creating a string array of size 2
		
        //we will get the number of titles which is N
		String line_String = lineText.toString(); //converting from text to string
		if (line_String.contains("<title>") && line_String.contains("</title>")) {
			int data_start = lineText.find("<title>");      //storing the indices of start point where we find <title>        
			int data_end = lineText.find("</title>", data_start); //storing indices of end point
			data_start = data_start + 7;                         //will start reading data from seventh point of the line
			int x = data_end - data_start;  //storing number of bytes of information
			if (data_start != data_end && data_end > data_start) {              //suppose we find title between two tags
				get_title[0] = Text.decode(lineText.getBytes(), data_start, x);  //we will get text from start point  
			} else
				get_title[0] = " "; //else put it as an empty string
		} else
		get_title[0] = " ";
		if(get_title[0].equals(" "))   //suppose we cant find a title
		context.write(new Text("temporary"), new IntWritable(0)); //we write count as 0
		else
			context.write(new Text("temporary"), new IntWritable(1)); //or we write count as 1
	}
}
